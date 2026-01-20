# Plan de Migration - Customer 360 → BigQuery

## 🎯 Objectif

Migrer Customer 360 de PostgreSQL (local) vers BigQuery (cloud) en 20 jours, 1h/jour.

## 📅 Roadmap (4 semaines)

### **Semaine 1 : Analyse & Setup**
- Documenter le système actuel ✅ (Jour 1)
- Créer compte Google Cloud (Jour 2)
- Analyser le schéma PostgreSQL (Jour 3)
- Designer l'architecture BigQuery (Jour 4)
- Baseline data quality (Jour 5)

### **Semaine 2 : Pipeline de Migration**
- Installer librairies Python (Jour 6)
- Script Extract (PostgreSQL → CSV) (Jour 7)
- Script Load (CSV → BigQuery) (Jour 8)
- Schemas BigQuery (Jour 9)
- Script Reconciliation (Jour 10)

### **Semaine 3 : dbt Migration**
- Migrer toutes les raw tables (Jour 11)
- Setup dbt-bigquery (Jour 12)
- Staging models (Jours 13-14)
- Intermediate models (Jour 15)

### **Semaine 4 : Analytics & Optimisation**
- Mart models (Jours 16-17)
- Performance tuning (Jour 18)
- Export Power BI (Jour 19)
- Documentation finale (Jour 20)

## ✅ Critères de Succès

| Métrique | PostgreSQL | BigQuery (cible) |
|----------|-----------|------------------|
| Clients uniques | 4,501 | 4,501 |
| VIP customers | 450 | 450 |
| Total revenue | €1.8M | €1.8M |
| Query time | 500ms | <100ms |
| Monthly cost | €50 (serveur) | €0 (free tier) |

## 🔒 Stratégie de Rollback

Si problème → PostgreSQL reste la source de vérité pendant toute la migration.
