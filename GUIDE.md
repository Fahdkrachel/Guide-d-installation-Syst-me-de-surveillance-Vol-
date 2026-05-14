# ✈️ Guide d'installation — Système de surveillance Vol AV-247

## Ce que tu vas construire

```
Capteurs Python → Kafka → Job Flink → ClickHouse → Grafana
```

Tout tourne dans Docker. Tu n'installes rien d'autre que Docker.

---

## Étape 0 — Prérequis : installer Docker

### Sur Windows
1. Va sur https://www.docker.com/products/docker-desktop
2. Télécharge **Docker Desktop for Windows**
3. Lance l'installateur, redémarre si demandé
4. Ouvre Docker Desktop — attends que la baleine verte apparaisse

### Sur Mac
1. Va sur https://www.docker.com/products/docker-desktop
2. Télécharge **Docker Desktop for Mac** (choisis Apple Silicon si M1/M2)
3. Glisse dans Applications, lance, attends la baleine verte

### Sur Linux (Ubuntu)
```bash
sudo apt update
sudo apt install -y docker.io docker-compose-plugin
sudo usermod -aG docker $USER
# Déconnecte-toi et reconnecte-toi, puis :
docker --version   # doit afficher une version
```

### Vérifier que Docker marche
```bash
docker --version
docker compose version
```
Tu dois voir quelque chose comme `Docker version 24.x` et `Docker Compose version v2.x`

---

## Étape 1 — Structure des fichiers

Copie tous les fichiers dans ce dossier :

```
aviation-iot/
│
├── docker-compose.yml          ← orchestre tous les services
│
├── simulator/
│   ├── simulator.py            ← génère les données capteurs
│   ├── requirements.txt
│   └── Dockerfile
│
├── flink/
│   ├── job.py                  ← analyse Kafka → ClickHouse
│   ├── requirements.txt
│   └── Dockerfile
│
├── clickhouse/
│   └── init.sql                ← crée les tables au démarrage
│
└── grafana/
    └── provisioning/
        ├── datasources/
        │   └── clickhouse.yml  ← connexion Grafana → ClickHouse
        └── dashboards/
            ├── dashboards.yml
            └── aviation.json   ← le dashboard préconfiguré
```

---

## Étape 2 — Démarrer le système

Ouvre un terminal dans le dossier `aviation-iot/` et lance :

```bash
# Construire les images Docker (1ère fois : ~3-5 minutes)
docker compose build

# Démarrer tous les services en arrière-plan
docker compose up -d
```

Tu vas voir des lignes comme :
```
✔ Container zookeeper          Started
✔ Container kafka              Started
✔ Container clickhouse         Started
✔ Container flink-jobmanager   Started
✔ Container flink-taskmanager-1 Started
✔ Container flink-taskmanager-2 Started
✔ Container grafana            Started
✔ Container sensor-simulator   Started
✔ Container flink-job          Started
```

---

## Étape 3 — Vérifier que tout marche

### Vérifier les conteneurs actifs
```bash
docker compose ps
```
Tous les services doivent être `running` (pas `exited`).

### Voir les logs du simulateur (les capteurs qui envoient des données)
```bash
docker logs sensor-simulator -f
```
Tu dois voir :
```
✈️  SIMULATEUR CAPTEURS — Vol AV-247
✅ Connecté à Kafka (kafka:29092)
📡 t=10s — 8 capteurs envoyés — carburant=94.9%
📡 t=20s — 8 capteurs envoyés — carburant=94.8%
🟠 ALERTE [temperature] = 921.3 °C (sévérité 1)
```

### Voir les logs du job Flink (analyse en cours)
```bash
docker logs flink-job -f
```
Tu dois voir :
```
✅ Connecté à ClickHouse
✅ Abonné à 8 topics Kafka
▶️  Lecture des topics Kafka en cours...
📊 100 messages traités
🚨 Alerte écrite [temperature] type=Seuil val=921.3 sev=1
```

---

## Étape 4 — Accéder aux interfaces

| Interface     | URL                        | Login / Mot de passe  |
|---------------|----------------------------|-----------------------|
| **Grafana**   | http://localhost:3000      | admin / aviation2024  |
| **Kafka UI**  | http://localhost:8080      | (aucun)               |
| **Flink UI**  | http://localhost:8081      | (aucun)               |
| **ClickHouse**| http://localhost:8123/play | (aucun)               |

### Dans Grafana
1. Va sur http://localhost:3000
2. Connecte-toi avec `admin` / `aviation2024`
3. Clique sur **Dashboards** dans le menu gauche
4. Ouvre **Vol AV-247 — Surveillance temps réel**
5. Tu vois les 8 jauges + courbes + tableau d'alertes

---

## Étape 5 — Tester les commandes utiles

### Voir les topics Kafka créés
```bash
docker exec kafka kafka-topics.sh \
  --bootstrap-server localhost:29092 --list
```

### Lire les messages d'un topic en direct
```bash
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 \
  --topic topic-temperature --from-beginning --max-messages 5
```

### Interroger ClickHouse directement
```bash
# Voir les 10 dernières mesures
docker exec clickhouse clickhouse-client \
  --query "SELECT capteur_id, valeur_brute, z_score, is_anomaly
           FROM iot.sensor_data
           ORDER BY timestamp DESC LIMIT 10"

# Voir les alertes récentes
docker exec clickhouse clickhouse-client \
  --query "SELECT * FROM iot.alerts ORDER BY timestamp DESC LIMIT 5"

# Compter les anomalies par capteur
docker exec clickhouse clickhouse-client \
  --query "SELECT capteur_id, count() as nb_anomalies
           FROM iot.sensor_data
           WHERE is_anomaly = 1
           GROUP BY capteur_id
           ORDER BY nb_anomalies DESC"
```

---

## Étape 6 — Arrêter et redémarrer

```bash
# Arrêter tous les conteneurs (sans supprimer les données)
docker compose stop

# Redémarrer
docker compose start

# Tout arrêter ET supprimer les conteneurs
docker compose down

# Tout supprimer y compris les données (repart de zéro)
docker compose down -v
```

---

## Résolution des problèmes courants

### ❌ "port already in use"
Un autre programme utilise le port. Solutions :
```bash
# Voir qui utilise le port 3000 (Grafana)
lsof -i :3000        # Mac/Linux
netstat -ano | findstr :3000  # Windows

# Ou changer le port dans docker-compose.yml
# Exemple : "3001:3000" au lieu de "3000:3000"
```

### ❌ Le simulateur ne se connecte pas à Kafka
```bash
# Vérifier que Kafka est bien démarré
docker logs kafka | tail -20

# Redémarrer uniquement le simulateur
docker compose restart simulator
```

### ❌ Grafana ne voit pas les données
```bash
# Vérifier que des données sont dans ClickHouse
docker exec clickhouse clickhouse-client \
  --query "SELECT count() FROM iot.sensor_data"

# Si 0, attendre 30 secondes et réessayer
# Le job Flink a besoin d'un peu de temps pour démarrer
```

### ❌ "no space left on device"
Docker manque d'espace disque :
```bash
# Nettoyer les images et conteneurs inutilisés
docker system prune -a
```

---

## Comment fonctionne le système (résumé)

```
1. simulator.py  →  génère 8 valeurs/seconde
                    et les envoie dans 8 topics Kafka

2. kafka         →  stocke les messages dans 3 partitions
                    avec réplication pour la fiabilité

3. job.py        →  lit chaque message
                    calcule moyenne (30s ou 60s)
                    calcule z-score
                    détecte anomalies
                    écrit dans ClickHouse

4. clickhouse    →  stocke tout dans iot.sensor_data
                    et les alertes dans iot.alerts

5. grafana       →  interroge ClickHouse toutes les 5s
                    et met à jour le dashboard
```

---

## Comprendre les anomalies simulées

Le simulateur injecte automatiquement une anomalie toutes les ~2 minutes
sur un capteur aléatoire pendant 10 secondes.

Tu verras dans les logs :
```
⚠️  ANOMALIE injectée sur [vibration] pendant 10 secondes
🔴 ALERTE [vibration] = 6.4 mm/s (sévérité 3) — 2024-01-15T10:32:03
✅ Anomalie [vibration] terminée
```

Et dans Grafana, la jauge passera au rouge et une ligne apparaîtra
dans le tableau des alertes.
