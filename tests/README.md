# Testy end-to-end

Zautomatyzowane testy e2e całego pipeline'u wykrywania anomalii:
`transactions` (Kafka) -> Flink detector -> `alerts` (Kafka) -> MongoDB.

Testy wysyłają deterministyczne transakcje do Kafki i sprawdzają, czy detektor
Flink generuje oczekiwane alarmy oraz czy dają się one zapisać do MongoDB.

## Wymagania

- Docker + Docker Compose
- Java 17 + Maven (do zbudowania JAR-a detektora)
- [uv](https://docs.astral.sh/uv/)

## Przygotowanie

1. Zbuduj JAR detektora (obraz Flinka kopiuje go podczas builda):

   ```bash
   mvn -f flink-detector/pom.xml clean package
   ```

2. Zainstaluj zależności testowe (izolowany `.venv` zarządzany przez uv):

   ```bash
   cd tests
   uv sync
   ```

## Uruchomienie

Z katalogu `tests/`:

```bash
uv run pytest e2e -v
```

Fixture `docker_stack` automatycznie:

- uruchamia cały stack (`docker compose up -d --build`),
- czeka aż Kafka odpowiada, topiki `transactions` i `alerts` istnieją,
  a job Flinka jest w stanie `RUNNING`,
- po testach sprząta środowisko (`docker compose down -v`).

## Uruchomienie na działającym stacku

Jeśli stack jest już uruchomiony i nie chcesz go restartować przy każdym przebiegu:

```bash
E2E_SKIP_DOCKER=1 uv run pytest e2e -v
```

## Zmienne środowiskowe

| Zmienna             | Domyślnie                                      | Opis                                  |
| ------------------- | ---------------------------------------------- | ------------------------------------- |
| `E2E_SKIP_DOCKER`   | `0`                                            | `1` = nie uruchamiaj/nie zatrzymuj stacku |
| `E2E_KAFKA_BOOTSTRAP` | `localhost:9092`                             | Adres brokerów Kafki                  |
| `E2E_FLINK_REST`    | `http://localhost:8081`                        | REST API Flinka                       |
| `E2E_MONGO_URI`     | `mongodb://admin:admin123@localhost:27017`     | URI MongoDB                           |

## Scenariusze

| Test                                   | Wejście                                   | Oczekiwany alarm             |
| -------------------------------------- | ----------------------------------------- | ---------------------------- |
| `test_limit_exceeded_detected`         | warmup + `amount > available_limit`       | `LIMIT_EXCEEDED`             |
| `test_statistical_amount_anomaly_detected` | warmup stabilnych kwot + nagły skok   | `STATISTICAL_AMOUNT_ANOMALY` |
| `test_new_location_detected`           | warmup w jednej lokalizacji + nowa lokalizacja | `NEW_LOCATION`          |
| `test_alert_persisted_to_mongo`        | wygenerowany alarm -> zapis do MongoDB    | dokument w kolekcji `alerts` |

## Uwagi

- Detektor wymaga co najmniej 5 wcześniejszych transakcji dla danej karty,
  zanim aktywuje detekcję statystyczną i nowej lokalizacji (stąd `warmup`).
- Każdy test używa unikalnego `card_id`, ponieważ Flink trzyma stan per karta.
- Pierwsze alarmy mogą pojawić się z kilkunastosekundowym opóźnieniem
  (rozgrzewka joba), dlatego polling alertów ma własny timeout.
