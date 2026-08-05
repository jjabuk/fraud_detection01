### Krok 1: Fundamenty i IaC (Infrastructure as Code)
*   [x] Inicjalizacja repozytorium GitHub (struktura katalogów: `data/`, `notebooks/`, `src/`, `infra/`, `tests/`).
*   [x] Konfiguracja środowiska wirtualnego (np. `uv` lub `Poetry`).
*   [x] Napisanie skryptów **OpenTofu (Terraform)**, które tworzą zasoby w GCP: 
    *   Dataset w BigQuery.
    *   Service Account z uprawnieniami do BigQuery i Cloud Run.
*   [ ] Ustawienie GitHub Actions (podstawowy linter np. `ruff`).

### Krok 2: Data Ingestion (Dagster)
*   [ ] Instalacja i konfiguracja lokalnego **Dagstera**.
*   [ ] Napisanie pierwszego zasobu (Software-Defined Asset), który pobiera surowe dane CSV z GitHuba / Kaggle i ładuje je do tabeli surowej (Raw) w BigQuery.
*   [ ] Walidacja schematu danych na wejściu.

### Krok 3: Feature Engineering Pipeline
*   [ ] Napisanie zasobów Dagstera, które transformują dane z tabeli Raw.
*   [ ] Implementacja logiki agregacji czasowych (np. liczba transakcji z danej karty w oknie 24h). Ważne: agregacje muszą być point-in-time, aby zapobiec wyciekowi danych!
*   [ ] Zapisanie przetworzonych cech do nowej tabeli w BigQuery (Feature Store).

### Krok 4: Model Training i Tracking (MLflow)
*   [ ] Postawienie lokalnego serwera **MLflow** (lub spięcie z chmurą).
*   [ ] Napisanie zasobu Dagstera odpowiedzialnego za podział zbioru (Split) na train/val/test w oparciu o oś czasu.
*   [ ] Implementacja prostego modelu (Baseline) i natychmiastowe zintegrowanie go z MLflow (`mlflow.log_params`, `mlflow.log_metrics`).
*   [ ] Wymiana Baseline'u na docelowy **LightGBM**, dodanie kalibracji prawdopodobieństwa, zapisanie artefaktu modelu do MLflow Registry.

### Krok 5: Model Serving (FastAPI)
*   [ ] Stworzenie mikroserwisu **FastAPI**, który udostępnia endpoint `POST /predict`.
*   [ ] Logika w FastAPI, która dynamicznie pobiera najnowszą produkcyjną wersję modelu z MLflow.
*   [ ] Zapakowanie API w kontener **Docker** (`Dockerfile`).

### Krok 6: Deployment i Monitoring
*   [ ] Rozszerzenie CI/CD (GitHub Actions) o budowanie obrazu Docker i wypychanie go do GCP Artifact Registry.
*   [ ] Wdrożenie kontenera na **Google Cloud Run**.
*   [ ] Modyfikacja kodu FastAPI, aby asynchronicznie zapisywał zapytania i predykcje do oddzielnej tabeli w BigQuery.
*   [ ] (Bonus) Prosty skrypt obliczający Population Stability Index (PSI) na tabeli logów, ostrzegający o dryfie danych.
