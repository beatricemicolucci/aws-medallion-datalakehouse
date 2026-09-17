# Gold Loader

Loader incrementale, configuration-driven, da Amazon Athena (Silver) a PostgreSQL (Gold).

## Setup

```bash
npm install
cp .env.example .env   # compila AWS_REGION, ATHENA_*, DATABASE_URL
npm test                # 35 unit test, nessuna connessione reale richiesta
npm start
```

Per provare una sola tabella alla volta, valorizzare `GOLD_LOADER_TABLE` nel file `.env` con il nome della tabella Athena o della tabella PostgreSQL di destinazione, per esempio `GOLD_LOADER_TABLE=anaclienti`. Se la variabile è vuota o assente, vengono elaborate tutte le tabelle.

## Struttura

```
config/mappings.json   → mapping tabelle/colonne, PK, watermark, tipi Postgres
.env                    → SOLO configurazione ambiente (region, endpoint, credenziali)
src/
  config.js             → carica e valida mappings.json (pre-flight, nessuna chiamata di rete)
  athena.js             → query Athena, paginazione, retry, log bytes scanned
  postgres.js           → batch size sicuro, CREATE/ALTER, upsert, mai DROP
  watermark.js          → stato incrementale per tabella
  lock.js                → lock advisory Postgres (una sola istanza alla volta)
  index.js               → orchestratore
test/                   → unit test su funzioni pure (config, batch size, upsert, diff schema)
```

Aggiungere una tabella = aggiungere una voce a `config/mappings.json`. Il codice non va toccato.

## Decisioni tecniche principali

**Batch size sicuro**: calcolato per tabella come `min(batchSize configurato, floor(65535 / n_colonne))`. Con `anaclienti` (83 colonne) e batchSize=500 si resta a 500 (41.500 parametri); il meccanismo scatterebbe automaticamente con tabelle più larghe o batch size più alti.

**Schema evolution**: nuova colonna in Bronze → `ALTER TABLE ADD COLUMN` automatico. Colonna rimossa dal mapping → solo `WARN`, mai `DROP`. Mismatch di tipo o di Primary Key → **blocca** quella tabella con errore esplicito, non tenta correzioni automatiche (sono operazioni potenzialmente distruttive).

**Cancellazioni**: **non implementate**, deliberatamente. Silver non porta un segnale esplicito di cancellazione (un record che sparisce dai nuovi batch Bronze non viene marcato come eliminato), e il loader incrementale legge solo righe *modificate*, mai l'assenza di PK. Un `DELETE` basato su questo sarebbe rischioso. Le opzioni sicure (riconciliazione periodica full-scan con flag soft-delete, o un segnale esplicito dalla sorgente) sono documentate nel codice (`index.js`) ma non implementate, per non introdurre complessità non ancora giustificata da un requisito confermato.

**Concorrenza**: lock advisory Postgres (`pg_try_advisory_lock`) a livello di intero run, non per singola tabella — se un'istanza sta già girando, la seconda esce immediatamente senza fare nulla.

**Costi Athena**: watermark abilitato → scan filtrato (non l'intera tabella). Watermark assente/primo run → scan completo, inevitabile, loggato come tale. Ogni query logga `QueryExecutionId` e bytes scansionati quando disponibili. Il polling ha un timeout configurabile con `ATHENA_QUERY_TIMEOUT_SECONDS` (default: 3600 secondi); al timeout il watermark non viene aggiornato e il lock viene rilasciato dal blocco `finally` dell'orchestratore.

## Problema noto: `ordcli_r` (segnalato, non corretto)

`data_modifica` è **sia parte della Primary Key sia colonna di watermark**. Se il valore cambia per lo stesso record logico, l'`UPSERT` non lo riconosce come lo stesso record (la PK stessa è cambiata) e lo **inserisce come riga nuova**, lasciando la versione precedente come duplicato "orfano" in Gold.

Il loader **non corregge questo automaticamente**: la validazione (`src/config.js`) stampa un `WARN` esplicito all'avvio, ma la configurazione resta quella fornita finché la chiave logica reale di `ordcli_r` non viene verificata con chi conosce il modello dati sorgente.
