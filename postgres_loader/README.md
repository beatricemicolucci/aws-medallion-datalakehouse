# Gold Loader

Loader incrementale, configuration-driven, da Amazon Athena (Silver) a PostgreSQL (Gold).

## Setup

```bash
npm install
nano .env              # crea il file e compila AWS_REGION, ATHENA_*, DATABASE_URL
npm test                # 36 unit test, nessuna connessione reale richiesta
npm start
```

## Come funziona il caricamento veloce

Per dataset grandi si può abilitare il percorso PostgreSQL `COPY` con una staging temporanea e un merge transazionale:

```env
POSTGRES_LOAD_MODE=copy
```

In parole semplici:

1. Athena legge le righe della tabella Silver.
2. PostgreSQL le riceve con `COPY`, un trasferimento molto più efficiente di migliaia di `INSERT` separati.
3. Le righe vengono raccolte in una tabella temporanea, visibile solo alla connessione corrente.
4. Una singola operazione di merge aggiorna o inserisce i dati nella tabella Gold usando la Primary Key.
5. Il watermark viene aggiornato solo dopo il commit del merge.

La modalità predefinita resta il caricamento batch precedente. Per tornare al comportamento precedente, rimuovere la variabile oppure lasciarla vuota:

```env
POSTGRES_LOAD_MODE=
```

La modalità `COPY` è più veloce perché riduce drasticamente il numero di round-trip tra Node.js e PostgreSQL. Il compromesso è una transazione più lunga, maggiore uso temporaneo di spazio e WAL, e un errore che può richiedere il rollback dell'intero caricamento della tabella invece del solo batch corrente.

Per questo motivo va provata prima su una singola tabella:

```env
GOLD_LOADER_TABLE=anaclienti
POSTGRES_LOAD_MODE=copy
```

Prima di un caricamento massivo verificare spazio libero, WAL, timeout e durata della transazione. Se la modalità `COPY` fallisce, fermare il processo, svuotare `POSTGRES_LOAD_MODE` e rilanciare il loader. Per un caricamento da zero, eliminare anche l'eventuale watermark della tabella, altrimenti il loader potrebbe eseguire solo il caricamento incrementale.

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

**Batch size sicuro**: calcolato per tabella come `min(batchSize configurato, floor(65535 / n_colonne))`. Il valore configurato corrente è 2500; con `anaclienti` (83 colonne) il codice usa automaticamente 789 righe per batch, mentre con `pallet_r` (24 colonne) può usare 2500 righe. Questo limite evita di superare il numero massimo di parametri PostgreSQL.

**Schema evolution**: nuova colonna in Bronze → `ALTER TABLE ADD COLUMN` automatico. Colonna rimossa dal mapping → solo `WARN`, mai `DROP`. Mismatch di tipo o di Primary Key → **blocca** quella tabella con errore esplicito, non tenta correzioni automatiche (sono operazioni potenzialmente distruttive).

**Cancellazioni**: **non implementate**, deliberatamente. Silver non porta un segnale esplicito di cancellazione (un record che sparisce dai nuovi batch Bronze non viene marcato come eliminato), e il loader incrementale legge solo righe *modificate*, mai l'assenza di PK. Un `DELETE` basato su questo sarebbe rischioso. Le opzioni sicure (riconciliazione periodica full-scan con flag soft-delete, o un segnale esplicito dalla sorgente) sono documentate nel codice (`index.js`) ma non implementate, per non introdurre complessità non ancora giustificata da un requisito confermato.

**Concorrenza**: lock advisory Postgres (`pg_try_advisory_lock`) a livello di intero run, non per singola tabella — se un'istanza sta già girando, la seconda esce immediatamente senza fare nulla.

**Costi Athena**: watermark abilitato → scan filtrato (non l'intera tabella). Watermark assente/primo run → scan completo, inevitabile, loggato come tale. Ogni query logga `QueryExecutionId` e bytes scansionati quando disponibili. Il polling ha un timeout configurabile con `ATHENA_QUERY_TIMEOUT_SECONDS` (default: 3600 secondi); al timeout il watermark non viene aggiornato e il lock viene rilasciato dal blocco `finally` dell'orchestratore.

## `ordcli_r` e Primary Key

La configurazione corrente usa `codice_ordine` e `numero_riga` come Primary Key di `ordcli_r`; `data_modifica` resta una normale colonna e `rdatam` resta il watermark. In questo modo un aggiornamento della data modifica la riga esistente invece di creare una nuova combinazione di Primary Key.

Se la tabella PostgreSQL era stata creata con la vecchia Primary Key, il loader non la modifica automaticamente: la differenza viene rilevata e la tabella viene bloccata. La constraint deve essere migrata manualmente dopo aver verificato l'assenza di duplicati.
