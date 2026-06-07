# Polecenie projektu

**Celem projektu jest opracowanie oprogramowania służącego do wykrywania anomalii w transakcjach dokonywanych kartami płatniczymi.**

1. Należy zaprojektować architekturę rozwiązania.
2. Projekt oprogramowania powinien zawierać opis poszczególnych komponentów.
3. Należy opracować symulator anomalii.
4. Oprogramowanie powinno zawierać następujące komponenty:
   - a. Standardowe oprogramowanie Kafka, Flink i MongoDB.
   - b. Symulatora transakcji realizowanych za pomocą kart płatniczych, który jednocześnie będzie producentem wiadomości dla Kafki.
   - c. Testowego konsumenta Kafki, który pozwoli na analizę poprawności generowanych danych z wizualizacją danych.
   - d. Detektora anomalii (Aplikacji Flink), który będzie czytał dane z Kafki, wykrywał anomalie i wysyłał do wydzielonego topiku Kafki informacje o alarmach.
   - e. Programu do odczytu alarmów, wizualizacji i informowania o alarmach.
5. Założenia do symulatora kart płatniczych:
   - a. Transakcje będą generowane dla 10000 różnych kart.
   - b. Dane generowane powinny zawierać:
     - ID karty.
     - ID użytkownika (jeden użytkownik może mieć więcej niż jedną kartę).
     - Lokalizację transakcji w formie współrzędnych GPS.
     - Wartość transakcji.
     - Dostępny limit wydatków na karcie.
   - c. Dane powinny być generowane w formacie JSON.
   - d. Centralną częścią symulatora jest generator danych pozwalający na generowanie różnego typu anomalii, np. nagła zmiana wartości transakcji, nagła zmiana lokalizacji, częstość transakcji itd. Osoba realizująca projekt ma swobodę doboru typu anomalii i liczby anomalii.
6. Założenia do detektora anomalii:
   - a. Dane powinny być czytane z Kafki w trybie prawie rzeczywistym.
   - b. Metoda/metody detekcji anomalii powinny opierać się w miarę możliwości na algorytmach bazujących na statystykach.
   - c. Należy przewidzieć pamięć tymczasową, np. do przechowywania częstych lokalizacji.
7. Realizujący projekt decyduje o wyborze języka programowania.

## Forma oddania projektu

1. Projekt rozwiązania.
2. Dedykowane oprogramowanie.
3. Wyniki testów oprogramowania.
4. Demonstracja rozwiązania prowadzącemu.
