# election_x_analysis

Dieses Projekt analysiert politische Kommunikation auf X im Kontext des Bundestagswahlkampfs 2025. Ziel ist es, Kommunikationsverhalten ausgewählter politischer Akteure datenbasiert auszuwerten und zentrale Muster im Zeitverlauf sichtbar zu machen.

## Ziel

Untersucht werden drei zentrale Analyseperspektiven:

- Aktivität der Accounts im Zeitverlauf
- Tonalität der veröffentlichten Beiträge
- Kommunikationsmodus der Beiträge

Die Arbeit ist als reproduzierbare datenbasierte Studienarbeit konzipiert und verbindet praktische Softwareentwicklung mit einer nachvollziehbaren analytischen Auswertung.

## Datengrundlage

Die Datengrundlage besteht aus Beiträgen ausgewählter politischer Accounts auf X im Zeitraum vom 06.11.2024 bis zum 23.02.2025. Der Untersuchungszeitraum orientiert sich an zentralen politischen Ereignissen zwischen Regierungskrise und Bundestagswahl.

Für die Analyse werden ausschließlich Originalposts berücksichtigt. Replies und Retweets sind nicht Teil des finalen Analyse-Datensatzes.

## Methodik

Die Analyse basiert auf einer mehrstufigen Pipeline:

- Erhebung der Beitragsdaten über die X API
- Speicherung der Rohdaten in einer SQLite-Datenbank
- Datenbereinigung und fachliche Aufbereitung
- Erstellung eines vorbereiteten Analyse-Datensatzes
- Durchführung der Analysen zu Aktivität, Tonalität und Kommunikationsmodus
- Export tabellarischer Ergebnisse und Erstellung von Visualisierungen

Ein früher Testlauf diente ausschließlich der technischen Prüfung der Pipeline und wurde nicht als finale Analysegrundlage verwendet. Die finale Auswertung basiert auf einer bereinigten und konsistent neu erzeugten Datenbasis.

## Reproduzierbarkeit

Die Analyse kann über die bereitgestellten Python-Skripte vollständig reproduziert werden. Die Projektstruktur umfasst die Datenaufbereitung, Analyse und Visualisierung in klar getrennten Verarbeitungsschritten.

Für eine saubere Reproduktion sollten die Skripte in der vorgesehenen Reihenfolge aus dem Projektverzeichnis heraus ausgeführt werden.

## Projektinhalt

Das Repository enthält insbesondere:

- Skripte zur Datenerhebung und Datenaufbereitung
- Analyse-Skripte für die zentralen Untersuchungsdimensionen
- Visualisierungen der Ergebnisse
- die SQLite-Datenbank beziehungsweise abgeleitete Analyseartefakte
- eine dokumentierte Grundlage für die Erstellung des Management-Reports

## Hinweis

Dieses Projekt wurde im Rahmen einer datenbasierten Studienarbeit erstellt. Ziel ist eine transparente, nachvollziehbare und für Dritte verständliche Analyse politischer Kommunikation auf X.