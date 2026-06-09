---
title: Projekt PSD
subtitle: System wykrywania anomalii w transakcjach kart płatniczych
author:
  - Bartłomiej Niewiarowski
  - Krzysztof Fijałkowski
date: 04.06.2026
documentclass: article
geometry:
  - margin=1in
fontenc: T1
fontfamily: mlmodern
fontsize: 11pt
numbersections: true
---

# Cel projektu

Celem projektu było opracowanie systemu służącego do wykrywania anomalii w transakcjach realizowanych za pomocą kart płatniczych.

System został zaprojektowany w architekturze przetwarzania strumieniowego (stream processing) z wykorzystaniem:

* Apache Kafka
* Apache Flink
* MongoDB
* aplikacji wizualizacyjnej
* generatora danych symulujących rzeczywiste transakcje

Projekt umożliwia:

* generowanie transakcji dla 10 000 kart płatniczych,
* wykrywanie anomalii w czasie prawie rzeczywistym,
* analizę danych transakcyjnych,
* wizualizację alarmów i statystyk,
* przechowywanie danych historycznych.

---

# Założenia projektu

Projekt został wykonany zgodnie z założeniami:

## Generowanie danych

Symulator transakcji generuje dane dla 10 000 różnych kart płatniczych.

Każda transakcja zawiera:

* ID karty,
* ID użytkownika,
* lokalizację GPS,
* wartość transakcji,
* dostępny limit karty,
* czas wykonania transakcji,
* informację o wygenerowanej anomalii.

Dane są przesyłane w formacie JSON.

## Wykrywanie anomalii

Detektor anomalii analizuje dane w trybie prawie rzeczywistym.

W systemie zaimplementowano między innymi następujące typy anomalii:

* przekroczenie limitu karty,
* nietypowo wysoka wartość transakcji,
* nagła zmiana lokalizacji.

Detekcja opiera się na metodach statystycznych:

* średniej wartości transakcji,
* wariancji,
* odchyleniu standardowym,
* z-score.

## Pamięć tymczasowa

Apache Flink wykorzystuje mechanizm state management.

Przechowywane są między innymi:

* średnia wartość transakcji dla karty,
* liczba wcześniejszych transakcji,
* historia lokalizacji,
* liczba transakcji w krótkim czasie.

---

# Architektura rozwiązania

## Schemat działania aplikacji
![Schemat architektury](./PSD_DIAGRAM.png)

---

# Opis komponentów systemu

## Symulator transakcji

Symulator transakcji odpowiada za generowanie danych wejściowych.

Aplikacja:

* generuje transakcje dla 10 000 kart,
* tworzy realistyczne dane finansowe,
* generuje anomalie,
* wysyła dane do Apache Kafka.

Każda wiadomość jest przesyłana do topiku `transactions`.

Przykładowa wiadomość:

```json
{
  "transaction_id": "8d2c9f3a-3c5a-4b8e-a8e2-7d2f2a1c5b11",
  "timestamp": "2026-05-21T12:00:00Z",
  "card_id": "card-000154",
  "user_id": "user-000021",
  "location": {
    "latitude": 52.2297,
    "longitude": 21.0122
  },
  "amount": 250.50,
  "available_limit": 5000.00,
  "currency": "PLN",
  "is_anomaly": false,
  "anomaly_type": null
}
```

---

## Apache Kafka

Apache Kafka pełni funkcję brokera wiadomości.

W projekcie wykorzystywane są dwa podstawowe topiki:

| Topik        | Opis                                  |
| ------------ | ------------------------------------- |
| transactions | transakcje generowane przez symulator |
| alerts       | alarmy wygenerowane przez Flink       |

Kafka umożliwia:

* przetwarzanie strumieniowe,
* niezależność komponentów,
* skalowalność rozwiązania,
* przesyłanie danych w czasie rzeczywistym.

---

## Testowy konsument Kafka

Testowy konsument służy do analizy poprawności generowanych danych.

Aplikacja umożliwia:

* podgląd danych z topiku `transactions`,
* sprawdzenie poprawności JSON,
* analizę danych wejściowych,
* podstawową wizualizację transakcji.

Komponent był wykorzystywany podczas testowania poprawności działania symulatora.

---

## Apache Flink — detektor anomalii

Detektor anomalii jest najważniejszym komponentem systemu.

Aplikacja:

* odczytuje dane z Apache Kafka,
* analizuje transakcje,
* wykorzystuje algorytmy statystyczne,
* wykrywa anomalie,
* generuje alarmy.

### Wykorzystywane mechanizmy

Detektor wykorzystuje:

* średnią wartość transakcji,
* wariancję,
* odchylenie standardowe,
* z-score,
* analizę lokalizacji.

### Przykładowe anomalie

#### LIMIT_EXCEEDED

Alert występuje, gdy:

* `amount > available_limit`.

Reguła działa od pierwszej transakcji (nie wymaga historii).

#### STATISTICAL_AMOUNT_ANOMALY

Alert występuje, gdy jednocześnie spełnione są warunki:

* system ma historię co najmniej `5` wcześniejszych transakcji dla danej karty (`count >= 5`),
* odchylenie standardowe jest większe od zera (`stddev > 0`),
* wartość bezwzględna z-score przekracza próg `3.0`:
  `abs((amount - mean) / stddev) > 3.0`.

#### NEW_LOCATION

Alert występuje, gdy:

* system ma historię co najmniej `5` wcześniejszych transakcji dla danej karty (`count >= 5`),
* bieżąca lokalizacja nie była wcześniej widziana dla tej karty.

Lokalizacja jest normalizowana do współrzędnych zaokrąglonych do dwóch miejsc
po przecinku (`lat,lon`), żeby drobne wahania GPS nie tworzyły fałszywych nowych
lokalizacji.

> Uwaga: aktualna implementacja `AnomalyDetectorJob` generuje trzy typy alertów:
> `LIMIT_EXCEEDED`, `STATISTICAL_AMOUNT_ANOMALY`, `NEW_LOCATION`.
>
> Jeśli warunki kilku alertów są spełnione jednocześnie, finalnie zwracany jest
> jeden alert z priorytetem `NEW_LOCATION`.

### State Management

Apache Flink wykorzystuje stan do przechowywania:

* liczby transakcji,
* średniej kwoty,
* historii lokalizacji,
* statystyk użytkownika.

Dzięki temu możliwe jest wykrywanie nietypowych zachowań dla konkretnej karty.

---

## Aplikacja wizualizacyjna

Aplikacja wizualizacyjna odpowiada za prezentację alarmów.

Możliwości aplikacji:

* wyświetlanie alarmów,
* filtrowanie alarmów,
* analiza statystyk,
* prezentacja wykresów,
* analiza danych historycznych.

Wyświetlane informacje:

* ID transakcji,
* ID użytkownika,
* typ anomalii,
* lokalizacja,
* kwota,
* czas alarmu.

---

## MongoDB

MongoDB służy do przechowywania:

* wykrytych alarmów,
* danych historycznych,
* statystyk transakcji.

Baza danych umożliwia:

* późniejszą analizę,
* filtrowanie danych,
* przechowywanie historii alarmów.

---

# Uruchomienie systemu

## Wymagania

Do uruchomienia projektu wymagane są:

* Docker,
* Docker Compose,
* Java 17+,
* Maven,
* uv (zarządzanie środowiskiem i zależnościami Python),
* Python 3.9+ (automatycznie wykrywany przez uv).

---

# Budowanie aplikacji Java

W katalogu projektu należy wykonać:

```bash
cd flink-detector; mvn clean package
```

Po zakończeniu budowania plik `.jar` pojawi się w katalogu:

```text
target/
```

---

# Uruchomienie środowiska Docker

Uruchomienie wszystkich komponentów:

```bash
docker compose up -d
```

Sprawdzenie statusu kontenerów:

```bash
docker ps
```

Zatrzymanie środowiska:

```bash
docker compose down
```

## Adresy komponentów

Kafka:            localhost:9092
Flink Dashboard:  http://localhost:8081
MongoDB:          mongodb://admin:admin123@localhost:27017
Alert Dashboard:  http://localhost:8501

---

## Synchronizacja zależności Python (uv)

W osobnych terminalach lub sekwencyjnie wykonaj (dla komponentów uruchamianych lokalnie poza Docker Compose):

```bash
cd simulator && uv sync
```

```bash
cd consumer && uv sync
```

---


# Uruchomienie aplikacji wizualizacyjnej

Domyślnie aplikacja Streamlit uruchamia się razem z całym środowiskiem przez Docker Compose:

```bash
docker compose up -d
```

Dashboard będzie dostępny pod adresem:

```text
http://localhost:8501
```

Opcjonalnie, uruchomienie lokalne (poza kontenerem):

```bash
cd alert-app && uv run streamlit run app.py
```

W trybie lokalnym aplikacja również będzie dostępna pod adresem:

```text
http://localhost:8501
```

---

# Uruchomienie symulatora transakcji

Przykładowe uruchomienie:

```bash
cd simulator && uv run python app.py
```

Symulator zaczyna wysyłać dane do topiku Kafka `transactions`.

---

## Uruchomienie testowego konsumenta Kafka

Przykładowe uruchomienie:

```bash
cd consumer && uv run python app.py
```

Konsument odczytuje wiadomości z topiku `transactions`.

---



# Alerty w aktualnej implementacji

Poniżej znajduje się komplet alertów generowanych przez aktualny kod detektora (`AnomalyDetectorJob`):

| `anomaly_type` | Kiedy alert się uruchamia | Szczegół działania |
| --- | --- | --- |
| `LIMIT_EXCEEDED` | Gdy `amount > available_limit`. | Alert uruchamia się natychmiast dla pojedynczej transakcji, bez potrzeby historii. |
| `STATISTICAL_AMOUNT_ANOMALY` | Gdy dla danej karty jest co najmniej 5 wcześniejszych transakcji (`count >= 5`) oraz `z-score > 3.0`. | Detektor liczy średnią i wariancję metodą Welforda (`mean`, `m2`), wyznacza odchylenie standardowe i porównuje odchylenie kwoty od profilu historycznego. |
| `NEW_LOCATION` | Gdy dla danej karty jest co najmniej 5 wcześniejszych transakcji (`count >= 5`) i bieżąca lokalizacja nie była wcześniej obserwowana. | Klucz lokalizacji jest budowany jako zaokrąglone `lat,lon` do 2 miejsc po przecinku. W aktualnej kolejności reguł ten alert ma najwyższy priorytet i może nadpisać wykrycie kwotowe lub limitowe dla tej samej transakcji. |

### Przykładowe payloady alertów

```json
{
  "anomaly_type": "LIMIT_EXCEEDED",
  "reason": "Transaction amount exceeds available limit"
}
```

```json
{
  "anomaly_type": "STATISTICAL_AMOUNT_ANOMALY",
  "reason": "Transaction amount significantly differs from historical average"
}
```

```json
{
  "anomaly_type": "NEW_LOCATION",
  "reason": "Transaction from previously unseen location"
}
```

---

# Podsumowanie

W ramach projektu opracowano kompletny system wykrywania anomalii w transakcjach kart płatniczych.

System umożliwia:

* generowanie realistycznych danych,
* przetwarzanie strumieniowe,
* wykrywanie anomalii w czasie prawie rzeczywistym,
* analizę statystyczną,
* wizualizację alarmów,
* przechowywanie danych historycznych.

Projekt spełnia wymagania zadania i wykorzystuje nowoczesne technologie przetwarzania danych strumieniowych.

# Link do demo
https://drive.google.com/file/d/1GBcvcP2kurKY2JwGBLP3mM4bLQFcMgYN/view?usp=sharing

# Testy end-to-end

W projekcie przygotowano testy end-to-end (katalog `tests/e2e/`), które na uruchomionym stacku Docker Compose wysyłają transakcje do Kafki, weryfikują generowanie alertów przez Flinka (przekroczenie limitu, anomalia kwotowa, nowa lokalizacja) oraz zapis alertów w MongoDB.

Aby je uruchomić należy:
- uruchomić całą aplikację
- wejść do folderu test
- uruchomić testy komendą `uv run pytest e2e -v`

wynik jest następujący:
```bash
tests git:(main) uv run pytest e2e -v           
======================== test session starts ========================
cachedir: .pytest_cache
rootdir: /home/gambolkf/studia/mgr/psd_project/tests
configfile: pyproject.toml
plugins: timeout-2.3.1
collected 4 items                                                                                                                        

e2e/test_mongo_persist.py::test_alert_persisted_to_mongo PASSED             [ 25%]
e2e/test_pipeline.py::test_limit_exceeded_detected PASSED                   [ 50%]
e2e/test_pipeline.py::test_statistical_amount_anomaly_detected PASSED       [ 75%]
e2e/test_pipeline.py::test_new_location_detected PASSED                     [100%]

======================== 4 passed in 242.03s (0:04:02) ========================
```
