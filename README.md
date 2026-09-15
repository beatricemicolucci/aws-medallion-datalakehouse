# Silver Loader

Software Node.js che legge lo schema Bronze dal Glue Data Catalog,
genera dinamicamente le query Athena/Iceberg e popola/aggiorna la
Silver per i 6 dataset del progetto — senza mai modificare la Bronze.

## Prerequisiti sulla EC2

```bash
# Verifica se Node.js è già installato (probabile, visto che hai
# già il crawler Node.js sulla stessa macchina)
node --version   # serve Node 18+

# Se manca, installalo (Amazon Linux 2023):
sudo dnf install -y nodejs
```

## Permessi IAM necessari (in aggiunta a quelli già presenti)

Il Ruolo IAM della tua EC2 (usato dal crawler) probabilmente ha già
accesso a S3 Bronze/Silver e al Glue Data Catalog. Verifica che
includa ANCHE questi permessi Athena, altrimenti aggiungili:

```bash
cat > athena-permissions.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": ["glue:GetPartitions"],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::athena-results-dev-490874680158",
        "arn:aws:s3:::athena-results-dev-490874680158/*"
      ]
    }
  ]
}
EOF

aws iam put-role-policy \
  --role-name GlueCrawlerRole-dev \
  --policy-name AthenaQueryAccess \
  --policy-document file://athena-permissions.json
```

*(Sostituisci `GlueCrawlerRole-dev` con il nome reale del ruolo IAM
della tua EC2, se diverso.)*

## Deploy sulla EC2

Da locale, comprimi e carica il progetto:

```bash
zip -r silver-loader.zip silver-loader/ -x "*/node_modules/*"
scp -i tua-chiave.pem silver-loader.zip ec2-user@<IP-EC2>:~/
# oppure, se usi SSM invece di SSH, carica prima su S3 e poi
# scaricalo dalla EC2 con "aws s3 cp"
```

Sulla EC2:

```bash
unzip silver-loader.zip
cd silver-loader
npm install
```

## Configurazione

Apri `config.json` e verifica che region, nomi di database/bucket
corrispondano al tuo ambiente reale. È già precompilato con i
valori del progetto discussi finora.

## Uso

```bash
# Prova SENZA eseguire nulla (mostra solo le query generate)
node src/index.js --dry-run

# Esegui tutti i 5 dataset "normali" (ORDCLI_R escluso di default)
node src/index.js

# Esegui un solo dataset specifico
node src/index.js --dataset=a_mis

# Esegui ANCHE ORDCLI_R (richiede conferma esplicita, vedi sotto)
node src/index.js --dataset=ordcli_r --confirm-append-only

# Ricostruisce da zero una tabella Silver già caricata (es. le
# prime tabelle create prima che i controlli qualità esistessero)
node src/index.js --dataset=a_mis --rebuild
```

## Comportamento

- **Colonne 100% NULL**: escluse dinamicamente dalla Silver ad ogni
  esecuzione, interrogando Bronze in tempo reale (una sola scansione
  per dataset, non una query per colonna). Non sono scritte a mano
  in `config.json`: se una colonna ricomincia a essere valorizzata in
  futuro, il prossimo run la rileva da solo.

- **Anomalie nelle date**: ogni colonna di tipo `timestamp`/`date` è
  rilevata automaticamente dallo schema Glue (non serve elencarle a
  mano). Una riga viene **esclusa interamente** se una qualsiasi
  delle sue colonne data è fuori dal range configurato in
  `dateValidation` (default: 1900-01-01 — oggi + 10 anni).

- **Schema evolution**: se una tabella Silver esiste già e Bronze
  offre ora una colonna che prima era 100% NULL (quindi esclusa),
  viene aggiunta con `ALTER TABLE ADD COLUMNS` — operazione solo sui
  metadati, non riscrive i file esistenti. Le colonne non vengono
  mai rimosse automaticamente da una tabella già esistente.

- **Rebuild (`--rebuild`)**: cancella la tabella Silver (metadati +
  file S3) e la ricrea da zero applicando i controlli qualità
  correnti. Sicuro perché Bronze non viene mai toccato e conserva
  tutta la storia: usalo per "pulire" tabelle caricate prima che
  questi controlli esistessero.

- **Se la tabella Silver non esiste ancora**: caricamento iniziale
  completo, con filtri già applicati.

- **Se la tabella Silver esiste già**: solo le partizioni Bronze più
  recenti dell'ultimo stato vengono processate (incrementale).

- **Idempotenza**: rilanciare lo stesso comando più volte, senza
  nuovi dati in Bronze, produce lo stesso risultato. L'operazione di
  MERGE è naturalmente idempotente (un batch già processato non
  produce nuovi UPDATE/INSERT); un rebuild è deterministico perché
  ricalcola sempre dallo stesso Bronze immutabile.

- **ORDCLI_R è disabilitato di default**: essendo un caso
  concettualmente diverso (PK include il cursor), il software si
  rifiuta di processarlo finché non usi esplicitamente il flag
  `--confirm-append-only`. Vedi il campo `note` in `config.json`.

- **Un dataset che fallisce non blocca gli altri**: se una query
  fallisce, l'errore viene stampato nel riepilogo finale, ma il
  ciclo continua con i dataset successivi.

## Esecuzione periodica (opzionale)

Per farlo girare automaticamente ad ogni nuovo caricamento Bronze,
puoi usare un cron job:

```bash
crontab -e
# Esegui ogni giorno alle 6:00
0 6 * * * cd /home/ec2-user/silver-loader && /usr/bin/node src/index.js >> /home/ec2-user/silver-loader.log 2>&1
```

## File generati

Nessun file di stato locale: lo stato "ultimo batch processato"
viene letto ogni volta direttamente dalla tabella Silver stessa
tramite una query Athena, per evitare disallineamenti tra uno stato
salvato a parte e i dati realmente presenti.
