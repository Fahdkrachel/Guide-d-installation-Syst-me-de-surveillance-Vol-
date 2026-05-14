"""
=============================================================
  JOB FLINK — Analyse temps réel des capteurs AV-247
=============================================================
  Ce script tourne dans le conteneur flink-job.
  Il lit les 8 topics Kafka, calcule pour chaque message :
    - La moyenne glissante (fenêtre 30s)
    - Le Z-score (détection d'anomalie)
    - La comparaison aux seuils critiques
  Puis écrit les résultats dans ClickHouse.

  NOTE : Pour simplifier le déploiement Docker pour débutants,
  ce job utilise kafka-python + clickhouse-driver directement
  (sans l'API Java Flink), ce qui donne les mêmes résultats
  de manière beaucoup plus lisible.
=============================================================
"""

import os
import json
import time
import math
import threading
from datetime import datetime
from collections import deque
from kafka import KafkaConsumer
from clickhouse_driver import Client

# ─────────────────────────────────────────────
#  Configuration
# ─────────────────────────────────────────────
KAFKA_BROKER     = os.getenv("KAFKA_BROKER",     "localhost:9092")
CLICKHOUSE_HOST  = os.getenv("CLICKHOUSE_HOST",  "localhost")
CLICKHOUSE_PORT  = int(os.getenv("CLICKHOUSE_PORT", "9000"))
VOL_ID           = os.getenv("VOL_ID",           "AV-247")

# Taille des fenêtres glissantes
FENETRE_COURTE   = 30   # secondes → TM1, TM2
FENETRE_LONGUE   = 60   # secondes → TM3, TM4

# Seuil Z-score au-delà duquel c'est une anomalie
ZSCORE_SEUIL     = 3.0

# Les 8 topics à écouter
TOPICS = [
    "topic-temperature",
    "topic-vibration",
    "topic-altitude",
    "topic-vitesse",
    "topic-pression",
    "topic-oxygene",
    "topic-humidite",
    "topic-carburant",
]

# Capteurs avec fenêtre longue (60s)
FENETRE_LONGUE_CAPTEURS = {"altitude", "vitesse", "humidite", "carburant"}

# Capteurs surveillés en corrélation croisée
CORRELATIONS = [
    ("humidite", "oxygene"),    # doivent évoluer ensemble
    ("temperature", "vibration"), # si temp monte, vibration aussi
]

# ─────────────────────────────────────────────
#  Mémoire des fenêtres glissantes
#  Pour chaque capteur : une file des N dernières valeurs
# ─────────────────────────────────────────────
fenetres = {
    topic.replace("topic-", ""): deque(maxlen=60)
    for topic in TOPICS
}


def connecter_clickhouse() -> Client:
    """Connexion à ClickHouse avec retry."""
    while True:
        try:
            client = Client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                database="iot",
            )
            client.execute("SELECT 1")
            print("✅ Connecté à ClickHouse")
            return client
        except Exception as e:
            print(f"⏳ ClickHouse pas prêt, retry dans 5s... ({e})")
            time.sleep(5)


def connecter_kafka() -> KafkaConsumer:
    """Connexion au consumer Kafka avec retry."""
    while True:
        try:
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                group_id="flink-job-aviation",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                consumer_timeout_ms=-1,  # bloquer indéfiniment
            )
            print(f"✅ Abonné à {len(TOPICS)} topics Kafka")
            return consumer
        except Exception as e:
            print(f"⏳ Kafka pas prêt, retry dans 5s... ({e})")
            time.sleep(5)


# ─────────────────────────────────────────────
#  CALCULS ANALYTIQUES
# ─────────────────────────────────────────────

def calculer_moyenne_std(valeurs: deque):
    """Calcule la moyenne et l'écart-type d'une fenêtre."""
    if len(valeurs) < 2:
        return 0.0, 1.0
    n    = len(valeurs)
    moy  = sum(valeurs) / n
    var  = sum((v - moy) ** 2 for v in valeurs) / n
    std  = math.sqrt(var) if var > 0 else 1.0
    return round(moy, 4), round(std, 4)


def calculer_zscore(valeur: float, moy: float, std: float) -> float:
    """
    Z-score = (valeur - moyenne) / écart-type
    Mesure à combien d'écarts-types la valeur
    s'éloigne de la normale.
    """
    if std == 0:
        return 0.0
    return round(abs(valeur - moy) / std, 3)


def detecter_variation_brusque(fenetre: deque, seuil_delta: float) -> bool:
    """
    Détecte si la valeur a changé trop vite.
    Compare la dernière valeur à la moyenne des 10 premières.
    """
    if len(fenetre) < 15:
        return False
    debut = list(fenetre)[:10]
    moy_debut = sum(debut) / len(debut)
    valeur_actuelle = fenetre[-1]
    return abs(valeur_actuelle - moy_debut) > seuil_delta


def detecter_correlation(nom_a: str, nom_b: str) -> bool:
    """
    Vérifie si deux capteurs évoluent dans le même sens.
    Si l'un monte et l'autre descend → corrélation cassée → anomalie.
    """
    f_a = fenetres[nom_a]
    f_b = fenetres[nom_b]
    if len(f_a) < 10 or len(f_b) < 10:
        return False  # pas assez de données

    # Tendance = différence entre la 2e moitié et la 1ère moitié
    moitie = len(f_a) // 2
    tendance_a = sum(list(f_a)[moitie:]) - sum(list(f_a)[:moitie])
    tendance_b = sum(list(f_b)[moitie:]) - sum(list(f_b)[:moitie])

    # Corrélation cassée = tendances de signes opposés
    return (tendance_a > 0) != (tendance_b > 0)


# ─────────────────────────────────────────────
#  ÉCRITURE DANS CLICKHOUSE
# ─────────────────────────────────────────────

def ecrire_sensor_data(client: Client, msg: dict, moy: float,
                       zscore: float, is_anomaly: int):
    """Écrit une ligne dans iot.sensor_data."""
    try:
        client.execute(
            """
            INSERT INTO iot.sensor_data
            (vol_id, timestamp, capteur_id, topic, valeur_brute,
             valeur_normalisee, z_score, moyenne_fenetre, is_anomaly)
            VALUES
            """,
            [(
                msg["vol_id"],
                datetime.fromisoformat(msg["timestamp"]),
                msg["capteur"],
                msg["topic"],
                msg["valeur"],
                round(msg["valeur"] / moy if moy != 0 else 1.0, 4),
                zscore,
                moy,
                is_anomaly,
            )],
        )
    except Exception as e:
        print(f"  ❌ Erreur écriture sensor_data : {e}")


def ecrire_alerte(client: Client, msg: dict, type_alerte: str,
                  zscore: float, seuil: float, severite: int):
    """Écrit une ligne dans iot.alerts."""
    try:
        messages_alerte = {
            1: f"Valeur anormale sur {msg['capteur']} : {msg['valeur']} {msg['unite']}",
            2: f"Attention critique sur {msg['capteur']} : {msg['valeur']} {msg['unite']}",
            3: f"URGENCE — {msg['capteur']} hors limites : {msg['valeur']} {msg['unite']}",
        }
        client.execute(
            """
            INSERT INTO iot.alerts
            (vol_id, timestamp, capteur_id, topic, type_alerte,
             valeur_mesuree, seuil, severite, message_alerte)
            VALUES
            """,
            [(
                msg["vol_id"],
                datetime.fromisoformat(msg["timestamp"]),
                msg["capteur"],
                msg["topic"],
                type_alerte,
                msg["valeur"],
                seuil,
                severite,
                messages_alerte.get(severite, "Anomalie détectée"),
            )],
        )
        print(f"  🚨 Alerte écrite [{msg['capteur']}] type={type_alerte} "
              f"val={msg['valeur']} sev={severite}")
    except Exception as e:
        print(f"  ❌ Erreur écriture alerte : {e}")


# ─────────────────────────────────────────────
#  TRAITEMENT D'UN MESSAGE
# ─────────────────────────────────────────────

# Seuils de variation brusque par capteur
SEUILS_VARIATION = {
    "altitude":    500,    # chute/montée > 500m en 60s = suspect
    "vitesse":     80,     # variation > 80 km/h = suspect
    "pression":    30,     # variation > 30 hPa = suspect
    "temperature": 50,     # variation > 50°C = suspect
    "vibration":   2.0,
    "oxygene":     3.0,
    "humidite":    15.0,
    "carburant":   5.0,
}

def traiter_message(msg: dict, client: Client):
    """
    Cœur du job Flink :
    1. Met à jour la fenêtre glissante
    2. Calcule moyenne, std, z-score
    3. Détecte anomalies (seuils, variation, corrélation)
    4. Écrit dans ClickHouse
    """
    nom    = msg["capteur"]
    valeur = msg["valeur"]

    # 1. Mettre à jour la fenêtre glissante
    fenetres[nom].append(valeur)

    # 2. Calculer statistiques
    moy, std = calculer_moyenne_std(fenetres[nom])
    zscore   = calculer_zscore(valeur, moy, std)

    # 3. Déterminer si c'est une anomalie
    is_anomaly = 1 if zscore >= ZSCORE_SEUIL else 0
    severite   = msg.get("severite", 0)

    # 4. Écrire la mesure dans sensor_data
    ecrire_sensor_data(client, msg, moy, zscore, is_anomaly)

    # 5. Écrire une alerte si nécessaire

    # Alerte Z-score
    if zscore >= ZSCORE_SEUIL:
        ecrire_alerte(client, msg, "Z-score", zscore,
                      ZSCORE_SEUIL, min(3, max(1, int(zscore - 1))))

    # Alerte seuil critique
    elif severite >= 1:
        ecrire_alerte(client, msg, "Seuil", zscore,
                      0, severite)

    # Alerte variation brusque (capteurs fenêtre longue)
    elif nom in FENETRE_LONGUE_CAPTEURS:
        seuil_v = SEUILS_VARIATION.get(nom, 999)
        if detecter_variation_brusque(fenetres[nom], seuil_v):
            ecrire_alerte(client, msg, "Variation brusque", zscore,
                          seuil_v, 2)


# ─────────────────────────────────────────────
#  THREAD DE CORRÉLATION CROISÉE (toutes les 30s)
# ─────────────────────────────────────────────

def thread_correlation(client: Client):
    """
    Vérifie les corrélations entre paires de capteurs
    toutes les 30 secondes.
    """
    while True:
        time.sleep(30)
        for nom_a, nom_b in CORRELATIONS:
            if detecter_correlation(nom_a, nom_b):
                print(f"  ⚡ Corrélation cassée : {nom_a} ↔ {nom_b}")
                # Créer un faux message pour l'alerte
                faux_msg = {
                    "vol_id":    VOL_ID,
                    "timestamp": datetime.utcnow().isoformat(),
                    "capteur":   f"{nom_a}+{nom_b}",
                    "topic":     f"topic-{nom_a}",
                    "valeur":    0.0,
                    "unite":     "",
                    "severite":  2,
                }
                ecrire_alerte(client, faux_msg, "Corrélation", 0.0, 0.0, 2)


# ─────────────────────────────────────────────
#  BOUCLE PRINCIPALE
# ─────────────────────────────────────────────

def main():
    print("=" * 55)
    print("  🔬 JOB FLINK — Analyse capteurs AV-247")
    print("=" * 55)

    client   = connecter_clickhouse()
    consumer = connecter_kafka()

    # Lancer le thread de corrélation en arrière-plan
    t = threading.Thread(
        target=thread_correlation, args=(client,), daemon=True
    )
    t.start()

    print("  ▶️  Lecture des topics Kafka en cours...\n")
    compteur = 0

    for record in consumer:
        try:
            msg = record.value
            traiter_message(msg, client)
            compteur += 1

            if compteur % 100 == 0:
                print(f"  📊 {compteur} messages traités")

        except Exception as e:
            print(f"  ❌ Erreur traitement message : {e}")


if __name__ == "__main__":
    main()
