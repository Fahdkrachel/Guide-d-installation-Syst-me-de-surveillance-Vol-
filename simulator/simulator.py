"""
=============================================================
  SIMULATEUR DE CAPTEURS — Vol AV-247
=============================================================
  Ce script simule 8 capteurs physiques d'un avion.
  Il envoie un message JSON dans Kafka toutes les secondes
  pour chaque capteur.

  Capteurs simulés :
    - temperature  : température moteur (°C)
    - vibration    : vibration moteur (mm/s)
    - altitude     : altitude (mètres)
    - vitesse      : vitesse air (km/h)
    - pression     : pression cabine (hPa)
    - oxygene      : niveau oxygène cabine (%)
    - humidite     : humidité cabine (%)
    - carburant    : niveau carburant (%)

  Fonctionnement :
    - Valeurs normales la plupart du temps
    - Toutes les ~2 minutes, une anomalie aléatoire
      est injectée sur un capteur (pour tester les alertes)
=============================================================
"""

import os
import json
import time
import random
import math
from datetime import datetime
from kafka import KafkaProducer

# ─────────────────────────────────────────────
#  Configuration depuis les variables d'env Docker
# ─────────────────────────────────────────────
KAFKA_BROKER   = os.getenv("KAFKA_BROKER",       "localhost:9092")
VOL_ID         = os.getenv("VOL_ID",             "AV-247")
INTERVAL       = float(os.getenv("SEND_INTERVAL_SEC", "1"))

# ─────────────────────────────────────────────
#  Définition des 8 capteurs
#  Chaque capteur a :
#    - topic    : le topic Kafka où envoyer les données
#    - mean     : valeur normale moyenne
#    - std      : variation normale (écart-type)
#    - unit     : unité de mesure
#    - seuil_1/2/3 : limites d'alerte
#    - min/max  : bornes physiques réalistes
# ─────────────────────────────────────────────
CAPTEURS = {
    "temperature": {
        "topic":   "topic-temperature",
        "mean":    880.0,   # °C moteur normal
        "std":     10.0,
        "unit":    "°C",
        "seuil_1": 900,
        "seuil_2": 940,
        "seuil_3": 970,
        "min":     800,
        "max":     1100,
    },
    "vibration": {
        "topic":   "topic-vibration",
        "mean":    2.1,     # mm/s normal
        "std":     0.3,
        "unit":    "mm/s",
        "seuil_1": 3.5,
        "seuil_2": 5.0,
        "seuil_3": 7.0,
        "min":     0,
        "max":     15,
    },
    "altitude": {
        "topic":   "topic-altitude",
        "mean":    10200.0, # mètres croisière
        "std":     50.0,
        "unit":    "m",
        "seuil_1": 10500,
        "seuil_2": 11000,
        "seuil_3": 11500,
        "min":     0,
        "max":     13000,
    },
    "vitesse": {
        "topic":   "topic-vitesse",
        "mean":    850.0,   # km/h croisière
        "std":     15.0,
        "unit":    "km/h",
        "seuil_1": 900,
        "seuil_2": 950,
        "seuil_3": 980,
        "min":     200,
        "max":     1100,
    },
    "pression": {
        "topic":   "topic-pression",
        "mean":    1013.0,  # hPa pression cabine
        "std":     5.0,
        "unit":    "hPa",
        "seuil_1": 950,
        "seuil_2": 900,
        "seuil_3": 850,
        "min":     700,
        "max":     1100,
    },
    "oxygene": {
        "topic":   "topic-oxygene",
        "mean":    21.0,    # % oxygène normal
        "std":     0.5,
        "unit":    "%",
        "seuil_1": 19.5,
        "seuil_2": 18.0,
        "seuil_3": 16.0,
        "min":     10,
        "max":     25,
    },
    "humidite": {
        "topic":   "topic-humidite",
        "mean":    45.0,    # % humidité cabine
        "std":     5.0,
        "unit":    "%",
        "seuil_1": 70,
        "seuil_2": 80,
        "seuil_3": 90,
        "min":     10,
        "max":     100,
    },
    "carburant": {
        "topic":   "topic-carburant",
        "mean":    75.0,    # % niveau carburant
        "std":     0.05,    # baisse très lentement
        "unit":    "%",
        "seuil_1": 30,
        "seuil_2": 20,
        "seuil_3": 10,
        "min":     0,
        "max":     100,
    },
}

# ─────────────────────────────────────────────
#  État interne des capteurs
#  (pour simuler des tendances réalistes)
# ─────────────────────────────────────────────
state = {
    nom: {"valeur": cfg["mean"], "anomalie_active": False}
    for nom, cfg in CAPTEURS.items()
}

# Le carburant descend au fil du temps
carburant_level = 95.0
tick = 0  # compteur de secondes


def generer_valeur(nom: str, cfg: dict) -> float:
    """
    Génère une valeur réaliste pour un capteur.
    - Valeur normale : bruit gaussien autour de la moyenne
    - Anomalie injectée : valeur qui dépasse les seuils
    """
    global carburant_level

    # Cas spécial carburant : descend progressivement
    if nom == "carburant":
        carburant_level -= random.uniform(0.001, 0.003)
        bruit = random.gauss(0, 0.05)
        return max(0, round(carburant_level + bruit, 2))

    # Vérifier si une anomalie est active sur ce capteur
    if state[nom]["anomalie_active"]:
        # Valeur 3 à 5 fois au-dessus de l'écart normal → anomalie
        delta = random.uniform(3.5, 5.0) * cfg["std"] * random.choice([-1, 1])
        valeur = cfg["mean"] + delta
    else:
        # Valeur normale avec légère dérive sinusoïdale (réaliste)
        derive = math.sin(tick / 60) * cfg["std"] * 0.5
        bruit  = random.gauss(0, cfg["std"])
        valeur = cfg["mean"] + derive + bruit

    # Borner la valeur aux limites physiques
    return round(max(cfg["min"], min(cfg["max"], valeur)), 2)


def calculer_severite(nom: str, valeur: float) -> int:
    """
    Retourne la sévérité selon les seuils définis.
    0 = normal, 1 = surveillance, 2 = attention, 3 = critique
    Pour pression et oxygène, l'alerte est en dessous du seuil.
    """
    cfg = CAPTEURS[nom]

    # Capteurs où l'alerte est à la BAISSE (pression, oxygène, carburant)
    if nom in ("pression", "oxygene", "carburant"):
        if valeur < cfg["seuil_3"]: return 3
        if valeur < cfg["seuil_2"]: return 2
        if valeur < cfg["seuil_1"]: return 1
    else:
        # Capteurs où l'alerte est à la HAUSSE
        if valeur > cfg["seuil_3"]: return 3
        if valeur > cfg["seuil_2"]: return 2
        if valeur > cfg["seuil_1"]: return 1

    return 0


def injecter_anomalie():
    """
    Toutes les ~2 minutes, déclenche une anomalie
    sur un capteur aléatoire pendant 10 secondes.
    """
    capteur = random.choice(list(CAPTEURS.keys()))
    state[capteur]["anomalie_active"] = True
    print(f"  ⚠️  ANOMALIE injectée sur [{capteur}] pendant 10 secondes")

    def desactiver():
        time.sleep(10)
        state[capteur]["anomalie_active"] = False
        print(f"  ✅ Anomalie [{capteur}] terminée")

    import threading
    threading.Thread(target=desactiver, daemon=True).start()


def connecter_kafka() -> KafkaProducer:
    """
    Connecte au broker Kafka avec retry.
    En Docker, Kafka démarre parfois après ce script.
    """
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",           # attendre confirmation d'écriture
                retries=5,
                linger_ms=10,         # regrouper les messages proches
            )
            print(f"✅ Connecté à Kafka ({KAFKA_BROKER})")
            return producer
        except Exception as e:
            print(f"⏳ Kafka pas encore prêt, nouvel essai dans 5s... ({e})")
            time.sleep(5)


# ─────────────────────────────────────────────
#  BOUCLE PRINCIPALE
# ─────────────────────────────────────────────
def main():
    global tick

    print("=" * 55)
    print("  ✈️  SIMULATEUR CAPTEURS — Vol AV-247")
    print("=" * 55)
    print(f"  Kafka     : {KAFKA_BROKER}")
    print(f"  Vol ID    : {VOL_ID}")
    print(f"  Intervalle: {INTERVAL}s")
    print(f"  Capteurs  : {len(CAPTEURS)}")
    print("=" * 55)

    producer = connecter_kafka()
    anomalie_timer = 0

    while True:
        tick += 1
        anomalie_timer += 1
        timestamp = datetime.utcnow().isoformat()

        # Déclencher une anomalie toutes les ~120 secondes
        if anomalie_timer >= 120:
            injecter_anomalie()
            anomalie_timer = 0

        # Envoyer un message pour chaque capteur
        for nom, cfg in CAPTEURS.items():
            valeur   = generer_valeur(nom, cfg)
            severite = calculer_severite(nom, valeur)

            # Le message JSON envoyé dans Kafka
            message = {
                "vol_id":    VOL_ID,
                "timestamp": timestamp,
                "capteur":   nom,
                "topic":     cfg["topic"],
                "valeur":    valeur,
                "unite":     cfg["unit"],
                "severite":  severite,
                "anomalie":  state[nom]["anomalie_active"],
            }

            # Envoyer dans le bon topic Kafka
            producer.send(cfg["topic"], value=message)

            # Afficher les alertes dans les logs
            if severite >= 2:
                emoji = "🔴" if severite == 3 else "🟠"
                print(f"  {emoji} ALERTE [{nom}] = {valeur} {cfg['unit']} "
                      f"(sévérité {severite}) — {timestamp}")

        producer.flush()

        # Afficher un log toutes les 10 secondes
        if tick % 10 == 0:
            print(f"  📡 t={tick}s — {len(CAPTEURS)} capteurs envoyés — "
                  f"carburant={carburant_level:.1f}%")

        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
