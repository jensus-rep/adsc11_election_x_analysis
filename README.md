# election_x_analysis

Dieses Repository dokumentiert eine datenbasierte Studienarbeit zur Analyse politischer Kommunikation auf X im Kontext des Bundestagswahlkampfs 2025. Ziel des Projekts ist es, das Kommunikationsverhalten ausgewählter politischer Akteure systematisch auszuwerten und zentrale Muster im Zeitverlauf sichtbar zu machen.

Das Projekt verbindet datenbasierte Analyse mit technischer Dokumentation, Reproduzierbarkeit und nachvollziehbarer Ergebnisaufbereitung.

## Projektziel

Im Mittelpunkt der Analyse stehen drei Perspektiven

1. Aktivität der Accounts im Zeitverlauf
2. Tonalität der veröffentlichten Beiträge
3. Kommunikationsmodus der Beiträge

Ziel ist es, nachvollziehbar darzustellen, wie sich politische Kommunikation im Verlauf des Wahlkampfs verändert und welche Muster sich in Aktivität, sprachlicher Tonalität und Kommunikationsform erkennen lassen.

## Untersuchungsgegenstand

Die Datengrundlage besteht aus Beiträgen ausgewählter politischer Accounts auf X im Zeitraum vom 06.11.2024 bis zum 23.02.2025. Der Untersuchungszeitraum beginnt mit dem Bruch der Ampelkoalition und endet mit der Bundestagswahl. Er erfasst damit eine Phase, die durch politische Krise, institutionelle Neuordnung und verdichtete Wahlkampfkommunikation geprägt ist.

Für die finale Analyse werden ausschließlich Originalposts berücksichtigt. Replies und Retweets sind nicht Bestandteil des finalen Analysedatensatzes.

Zur vergleichenden Auswertung wird der Untersuchungszeitraum in zwei Wahlkampfphasen unterteilt. Phase A umfasst den Zeitraum vom 06.11.2024 bis zum 26.12.2024 und bildet die Kommunikation nach dem Bruch der Regierungskoalition, aber vor der formalen Auflösung des Bundestages ab. Phase B umfasst den Zeitraum vom 27.12.2024 bis zum 23.02.2025 und erfasst die Kommunikation nach der Auflösung des Bundestages bis zur vorgezogenen Bundestagswahl. Die Phaseneinteilung folgt damit politischen Zäsuren im Untersuchungszeitraum.

## Methodischer Überblick

Die Analyse folgt einer modular aufgebauten Pipeline. Datenerhebung, Aufbereitung, Validierung, Analyse und Visualisierung werden systematisch voneinander getrennt. Ziel dieser Struktur ist es, technische Prüfbarkeit, Reproduzierbarkeit und fachliche Nachvollziehbarkeit zu erhöhen.

Zu Beginn des Projekts wurde testweise ein Webscraping Ansatz geprüft. Im Verlauf der technischen Umsetzung zeigte sich jedoch, dass dieser Zugang für eine saubere produktive Datenerhebung nicht hinreichend belastbar war, da der Zugriff auf Plattformdaten technisch eingeschränkt war. Die finale Datenerhebung wurde daher vollständig auf die offizielle X API gestützt.

Ein früher Testlauf diente ausschließlich der technischen Prüfung der Pipeline und wurde nicht als Grundlage der finalen Analyse verwendet.

## Datenbasis und Analysebasis

Die Rohdaten werden in einer SQLite Datenbank in der Tabelle `posts` gespeichert. Für die eigentliche Auswertung wird daraus die vorbereitete Tabelle `posts_prepared` erzeugt. Diese bildet die verbindliche Grundlage aller nachgelagerten Analysen und Visualisierungen.

Technisch wurden zehn Zielaccounts in die Datenerhebung aufgenommen. In der finalen Analysebasis sind acht Accounts mit beobachtbaren Originalposts im definierten Untersuchungszeitraum enthalten. Für zwei Zielaccounts konnten unter den gesetzten Analysebedingungen keine Originalposts festgestellt werden. Diese Fälle wurden als ausbleibende beobachtbare Aktivität und nicht als technischer Fehler interpretiert.

## Datenpipeline im Überblick

### 1. Setup und Rohdatenbasis

`01_setup_database.py`  
Legt die SQLite Datenbank an und erstellt die zentrale Tabelle `posts`.

`02_import_posts.py`  
Importiert Beitragsdaten aus einer CSV Datei in die Datenbank. Dieses Skript dient als alternative, quellenunabhängige Importschicht.

### 2. Account Lookup und Datenerhebung

`03_fetch_user_ids.py`  
Löst die Zielaccounts in X User IDs auf und erzeugt die Grundlage für die API gestützte Datenerhebung.

`04_collect_x_posts_api.py`  
Ruft Beiträge der ausgewählten politischen Accounts über die offizielle X API ab und speichert sie in der SQLite Datenbank. Für die Ausführung ist ein X Developer Zugang mit gültigem Bearer Token erforderlich. Die Nutzung dieses Schritts kann kostenpflichtig sein und sollte nur bewusst ausgeführt werden.

### 3. Datenbereinigung und Analysebasis

`05_sanitize_dataset.py`  
Bereinigt die Datenbank für die finale Analyse, entfernt Testdaten, vereinheitlicht Parteinamen und löscht problematische Datensätze.

`06_prepare_dataset.py`  
Erstellt aus der Rohdatentabelle die vorbereitete Analysetabelle `posts_prepared`. Dabei werden nur relevante Originalposts übernommen, Datumsfelder ergänzt und Wahlkampfphasen zugewiesen.

`07_validate_dataset.py`  
Validiert die finale Datenbasis technisch und prüft unter anderem Tabellenstruktur, Scope, Dublettenfreiheit und Konsistenz zwischen Rohdaten und Analysebasis.

### 4. Analysen

`08_analysis_activity.py`  
Erstellt Aggregationen zur Posting Aktivität nach Woche, Phase und Account.

`10_sentiment_analysis.py`  
Führt eine Sentiment Analyse als pragmatische Baseline durch und erstellt Aggregationen nach Phase und Account.

`12_communication_mode_analysis.py`  
Ordnet Beiträge regelbasiert Kommunikationsmodi zu und erstellt Aggregationen nach Phase und Account.

### 5. Visualisierungen

`09_visualize_activity.py`  
Erstellt Visualisierungen zur Aktivitätsanalyse.

`11_visualize_sentiment.py`  
Erstellt Visualisierungen zur Sentiment Analyse.

`13_visualize_communication_mode.py`  
Erstellt Visualisierungen zur Analyse des Kommunikationsmodus.

## Zentrale Analyseperspektiven

### Aktivität

Untersucht wird, wie sich die Kommunikationsaktivität der betrachteten Accounts im Zeitverlauf verteilt. Die Auswertung erfolgt unter anderem auf Wochen, Phasen und Account Ebene.

### Tonalität

Die Tonalität der Beiträge wird über eine nachvollziehbare Sentiment Logik als pragmatische Baseline ausgewertet. Ziel ist es, grobe Muster sprachlicher Valenz im Datensatz sichtbar zu machen.

### Kommunikationsmodus

Zusätzlich wird analysiert, in welchen Kommunikationsformen die Beiträge auftreten. Dafür wird eine regelbasierte und transparente Kategorisierung eingesetzt, um dominante Muster im Wahlkampfverlauf strukturiert auszuwerten.

## Voraussetzungen

Für die vollständige Reproduktion des Projekts werden insbesondere benötigt

1. eine Python Umgebung mit den erforderlichen Abhängigkeiten
2. SQLite
3. eine korrekt konfigurierte `.env` Datei für API Zugriffe
4. für `04_collect_x_posts_api.py` ein gültiger X Developer Zugang mit Bearer Token

## Empfohlene Ausführungsreihenfolge

Für eine saubere Reproduktion sollten die Skripte in der vorgesehenen Reihenfolge aus dem Projektverzeichnis heraus ausgeführt werden

1. `01_setup_database.py`
2. optional `02_import_posts.py`, falls Daten aus einer CSV Datei importiert werden sollen
3. `03_fetch_user_ids.py`
4. `04_collect_x_posts_api.py` mit X API Zugang und potenziell kostenpflichtiger Nutzung
5. `05_sanitize_dataset.py`
6. `06_prepare_dataset.py`
7. `07_validate_dataset.py`
8. `08_analysis_activity.py`
9. `09_visualize_activity.py`
10. `10_sentiment_analysis.py`
11. `11_visualize_sentiment.py`
12. `12_communication_mode_analysis.py`
13. `13_visualize_communication_mode.py`

## Wichtiger Hinweis zur Datenerhebung über die X API

Das Skript `04_collect_x_posts_api.py` setzt einen gültigen X Developer Zugang sowie einen Bearer Token in einer `.env` Datei voraus.

Zusätzlich ist zu beachten

1. Die produktive Datenerhebung über die offizielle X API kann kostenpflichtig sein.
2. Das Skript sollte nicht unbedacht erneut ausgeführt werden.
3. Vor produktiven Re Runs sollte geprüft werden, ob ausreichend API Credits verfügbar sind.
4. Das Skript ist so angelegt, dass es bei kritischen API Problemen, etwa bei fehlenden Credits, den Lauf sauber abbrechen kann.

Die finale Datenerhebung und Auswertung dieses Projekts basiert vollständig auf der offiziellen X API.

## Repository Inhalt

Das Repository enthält insbesondere

1. Skripte zur Datenerhebung, Aufbereitung und Validierung
2. Analyse Skripte für die zentralen Untersuchungsdimensionen
3. Skripte zur Visualisierung der Ergebnisse
4. die SQLite Datenbank und abgeleitete Analyseartefakte
5. tabellarische Ergebnisdateien
6. Abbildungen für Auswertung und Report
7. die technische Grundlage für die Erstellung des Management Reports

## Wissenschaftlicher und technischer Anspruch

Dieses Projekt wurde im Rahmen einer datenbasierten Studienarbeit erstellt. Ziel ist eine transparente, nachvollziehbare und für Dritte verständliche Analyse politischer Kommunikation auf X. Neben den inhaltlichen Ergebnissen stehen daher auch methodische Klarheit, technische Dokumentation und Reproduzierbarkeit im Mittelpunkt.

## Hinweis zur Einordnung

Die Ergebnisse dieser Arbeit beschreiben die beobachtete Kommunikation ausgewählter politischer Accounts im definierten Untersuchungszeitraum. Sie stellen keine repräsentative Abbildung der gesamten politischen Online Kommunikation in Deutschland dar und erlauben keine kausalen Aussagen über Wirkungen auf das Wahlverhalten.
