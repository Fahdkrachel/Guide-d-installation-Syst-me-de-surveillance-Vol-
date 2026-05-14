-- ============================================================
--  CLICKHOUSE — Initialisation base de données IoT
--  Ce fichier est exécuté automatiquement au démarrage
--  du conteneur ClickHouse.
-- ============================================================

-- Créer la base de données si elle n'existe pas
CREATE DATABASE IF NOT EXISTS iot;

-- ─────────────────────────────────────────────
--  TABLE 1 : iot.sensor_data
--  Stocke TOUTES les mesures de tous les capteurs.
--  1 ligne = 1 message d'un capteur.
--  Environ 8 lignes par seconde (8 capteurs × 1/s).
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS iot.sensor_data
(
    -- Identifiant du vol
    vol_id           String,

    -- Horodatage précis de la mesure
    timestamp        DateTime,

    -- Nom du capteur (ex: "temperature", "vibration")
    capteur_id       String,

    -- Topic Kafka d'origine (ex: "topic-temperature")
    topic            String,

    -- Valeur brute envoyée par le capteur
    valeur_brute     Float64,

    -- Valeur normalisée (valeur / moyenne)
    valeur_normalisee Float64,

    -- Z-score calculé par le job Flink
    -- (mesure l'écart à la normale)
    z_score          Float64,

    -- Moyenne glissante de la fenêtre (30s ou 60s)
    moyenne_fenetre  Float64,

    -- 0 = normal, 1 = anomalie détectée
    is_anomaly       UInt8
)
-- MergeTree : le moteur de stockage principal de ClickHouse
-- Optimisé pour les séries temporelles
ENGINE = MergeTree()
-- Partitionné par jour → requêtes rapides sur une plage de dates
PARTITION BY toYYYYMMDD(timestamp)
-- Trié par capteur + temps → lecture ultra-rapide des courbes
ORDER BY (capteur_id, timestamp)
-- Supprime automatiquement les données de plus de 30 jours
TTL timestamp + INTERVAL 30 DAY;


-- ─────────────────────────────────────────────
--  TABLE 2 : iot.alerts
--  Stocke uniquement les ALERTES détectées.
--  Beaucoup plus petite que sensor_data.
--  Utilisée pour le tableau d'alertes dans Grafana.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS iot.alerts
(
    vol_id           String,
    timestamp        DateTime,
    capteur_id       String,
    topic            String,

    -- Type d'alerte : "Z-score", "Seuil", "Variation brusque", "Corrélation"
    type_alerte      String,

    -- La valeur qui a déclenché l'alerte
    valeur_mesuree   Float64,

    -- Le seuil qui a été dépassé
    seuil            Float64,

    -- 1 = info, 2 = attention, 3 = critique
    severite         UInt8,

    -- Message lisible pour Grafana
    message_alerte   String,

    -- Index pour accélérer les filtres par capteur
    INDEX idx_capteur (capteur_id, timestamp) TYPE minmax GRANULARITY 4
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (severite, capteur_id, timestamp)
TTL timestamp + INTERVAL 90 DAY;


-- ─────────────────────────────────────────────
--  VUES UTILES — pour simplifier les requêtes Grafana
-- ─────────────────────────────────────────────

-- Vue : dernière valeur de chaque capteur
CREATE VIEW IF NOT EXISTS iot.derniere_valeur AS
SELECT
    capteur_id,
    argMax(valeur_brute, timestamp)   AS valeur,
    argMax(z_score, timestamp)        AS zscore,
    argMax(is_anomaly, timestamp)     AS anomalie,
    max(timestamp)                    AS derniere_maj
FROM iot.sensor_data
GROUP BY capteur_id;

-- Vue : résumé des alertes des dernières 24h
CREATE VIEW IF NOT EXISTS iot.alertes_recentes AS
SELECT
    capteur_id,
    type_alerte,
    severite,
    count()        AS nb_alertes,
    max(timestamp) AS derniere_alerte
FROM iot.alerts
WHERE timestamp >= now() - INTERVAL 24 HOUR
GROUP BY capteur_id, type_alerte, severite
ORDER BY severite DESC, nb_alertes DESC;


-- ─────────────────────────────────────────────
--  DONNÉES DE TEST — pour vérifier que tout marche
--  (insérées au démarrage, supprimées après 1 minute)
-- ─────────────────────────────────────────────
INSERT INTO iot.sensor_data VALUES
    ('AV-247', now(), 'temperature', 'topic-temperature', 882.0, 1.002, 0.4, 880.0, 0),
    ('AV-247', now(), 'vibration',   'topic-vibration',   2.1,   1.000, 0.1, 2.1,   0),
    ('AV-247', now(), 'altitude',    'topic-altitude',    10200, 1.000, 0.2, 10200, 0),
    ('AV-247', now(), 'vitesse',     'topic-vitesse',     850.0, 1.000, 0.1, 850.0, 0),
    ('AV-247', now(), 'pression',    'topic-pression',    1013.0,1.000, 0.1, 1013.0,0),
    ('AV-247', now(), 'oxygene',     'topic-oxygene',     21.0,  1.000, 0.0, 21.0,  0),
    ('AV-247', now(), 'humidite',    'topic-humidite',    45.0,  1.000, 0.1, 45.0,  0),
    ('AV-247', now(), 'carburant',   'topic-carburant',   95.0,  1.000, 0.0, 95.0,  0);

INSERT INTO iot.alerts VALUES
    ('AV-247', now(), 'temperature', 'topic-temperature',
     'Seuil', 963.0, 950.0, 3, 'URGENCE — temperature hors limites : 963.0 °C');
