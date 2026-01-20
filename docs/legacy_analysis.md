# Customer 360 Legacy System Analysis

## 🏢 Vue d'ensemble

**Plateforme actuelle :** PostgreSQL 14 (local, Docker)
**Volume de données :** ~5,000 clients
**Architecture :** 3-layer data warehouse

### Pourquoi 3 couches ?

1️⃣ **RAW** (Données brutes)
   - Ce qui arrive directement des CSV
   - AUCUNE transformation
   - Exemple : email = "  JOHN@GMAIL.COM  " (espaces, majuscules)

2️⃣ **STAGING** (Nettoyage)
   - Normalisation : email = "john@gmail.com"
   - Validation : is_valid_email = TRUE/FALSE
   - Parsing des dates : "2023-01-15" → DATE

3️⃣ **WAREHOUSE** (Analytics)
   - Agrégations business : RFM scores, Health scores
   - Dimensions : dim_customers (table de référence)
   - Métriques : cohort_retention (analyse de rétention)

---

## 📦 Inventaire des Tables

### RAW Layer (3 tables)
| Table | Contenu | Rows |
|-------|---------|------|
| `raw.csv_customers` | Données clients brutes | 5,437 |
| `raw.csv_orders` | Transactions brutes | ~15,000 |
| `raw.csv_products` | Catalogue produits | ~500 |

### STAGING Layer (3 tables)
| Table | Transformation | Output |
|-------|---------------|--------|
| `stg_csv_customers` | Email normalisé, dates parsées | 5,437 |
| `stg_csv_orders` | Montants validés, dates | ~15,000 |
| `stg_csv_products` | Prix nettoyés | ~500 |

### WAREHOUSE Layer (4 tables)
| Table | Business Logic | Insight |
|-------|---------------|---------|
| `dim_customers` | Déduplication (5,437 → 4,501) | Clients uniques |
| `customer_rfm` | Segmentation RFM | 450 VIPs, 800 Champions |
| `customer_health` | Score de santé (0-100) | Qui va churner ? |
| `cohort_retention` | Cohortes mensuelles | Taux de rétention |

---

## 🧮 Business Logic Clé à Préserver

### 1. Déduplication (Identity Resolution)

**Problème :** Jean achète avec "jean@gmail.com", puis "JEAN@GMAIL.COM"
→ PostgreSQL voit 2 clients différents !

**Solution actuelle :**
```sql
-- Normaliser l'email
email_normalized = LOWER(TRIM(email))

-- Créer une clé unique
identity_match_key = MD5(email_normalized)

-- Garder seulement le premier achat
ROW_NUMBER() OVER (PARTITION BY identity_match_key ORDER BY created_at)
```

**Résultat :** 5,437 rows → 4,501 clients uniques

---

### 2. RFM Segmentation

**RFM = Recency, Frequency, Monetary**

**Analogie du restaurant :**
- **Recency** : Dernière visite il y a combien de temps ?
  - 7 jours = client actif (score 5)
  - 6 mois = client perdu (score 1)

- **Frequency** : Combien de visites cette année ?
  - 50 fois = client fidèle (score 5)
  - 2 fois = occasionnel (score 1)

- **Monetary** : Combien dépensé au total ?
  - €10,000 = gros client (score 5)
  - €50 = petit client (score 1)

**Segments :**
```
VIP       : R≥4, F≥4, M≥4  (Vient souvent, dépense beaucoup, récent)
Champion  : R≥4, F≥3       (Très actif)
Loyal     : R≥3, F≥3       (Fidèle mais moins actif)
At Risk   : R≤2, F≥3       (Était fidèle, mais parti !)
Lost      : R≤2, F≤2       (Complètement inactif)
```

**Dans ton dataset :**
- 450 VIPs (10% des clients = 90% du revenu !)
- 800 Champions
- 200 "At Risk" → Marketing doit les réactiver !

---

### 3. Health Score

**Formule :**
```
Health Score = (Recency × 25) + (Frequency × 25) + (Monetary × 30) + (Email Valid × 20)
```

**Pourquoi ces poids ?**
- **Monetary = 30%** : Le plus important = combien ils dépensent
- **Recency = 25%** : S'ils sont partis = danger !
- **Frequency = 25%** : Fidélité
- **Email Valid = 20%** : Si email invalide = on ne peut pas les recontacter !

**Classification :**
- Excellent (≥80) : VIPs en bonne santé
- Good (≥60) : OK
- Fair (≥40) : À surveiller
- At Risk (<40) : DANGER ⚠️

---

## 🚨 Problèmes de Qualité Identifiés

### 1. Emails Malformés (63 cas)
```
Invalides :
- "john@" (pas de domaine)
- "@gmail.com" (pas de nom)
- "john.doe" (pas de @)
- "test@test" (domaine invalide)
```

### 2. Doublons (436 cas)
```
jean@gmail.com   → 3 fois
marie@yahoo.fr   → 2 fois
...
Total : 5,437 rows → 4,501 uniques
```

### 3. Dates Mixtes (3 formats !)
```
Format 1: "2023-01-15"      (ISO 8601)
Format 2: "15/01/2023"      (européen)
Format 3: "01-15-2023"      (américain)
```

**Solution actuelle :** Macro `parse_mixed_dates()` qui essaie les 3 formats

---

## 🔄 PostgreSQL → BigQuery : Traductions Nécessaires

### Syntaxe SQL

| PostgreSQL | BigQuery | Pourquoi ? |
|-----------|----------|-----------|
| `::DATE` | `CAST(x AS DATE)` | BigQuery n'aime pas `::` |
| `~` (regex) | `REGEXP_CONTAINS()` | Fonction différente |
| `VARCHAR(255)` | `STRING` | BigQuery = pas de limite de taille |
| `SERIAL` | `INT64` | Pas d'auto-increment |

### Exemple Concret

**PostgreSQL :**
```sql
SELECT 
  email::VARCHAR,
  created_at::DATE,
  CASE WHEN email ~ '^[A-Z]' THEN TRUE ELSE FALSE END
FROM raw.csv_customers;
```

**BigQuery :**
```sql
SELECT 
  CAST(email AS STRING),
  CAST(created_at AS DATE),
  REGEXP_CONTAINS(email, r'^[A-Z]')
FROM `customer360-migration.raw_data.csv_customers`;
```

---

## 📈 Métriques de Succès

**La migration est réussie si :**

✅ **Exactitude** : 4,501 clients uniques (même nombre qu'avant)
✅ **RFM** : 450 VIPs (pareil)
✅ **Revenue** : €1.8M total (pareil)
✅ **Performance** : Queries <100ms (vs 500ms en PostgreSQL)
✅ **Coût** : €0 (free tier BigQuery)

---

## 🎯 Conclusion

**Ce qu'on migre :**
- 10 tables
- 20,000+ rows au total
- 3 couches (raw → staging → warehouse)
- Business logic complexe (RFM, dedup, health scoring)

**Ce qu'on DOIT préserver :**
- Même nombre de clients uniques
- Même segmentation RFM
- Même logique de déduplication

**Ce qu'on va améliorer :**
- Performance (clustering, partitioning)
- Scalabilité (cloud vs local)
- Coût (€0 vs serveur)
