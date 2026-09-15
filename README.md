# Custom crawler S3 Parquet -> AWS Glue

Questa è una prima versione didattica del crawler. È un normale programma Node.js da eseguire su EC2: non usa AWS Glue Crawler e non usa Lambda.

## Cosa fa

1. Elenca tutti gli oggetti sotto il bucket/prefix S3, anche oltre 1.000 oggetti.
2. Considera la prima cartella come nome del dataset. Ad esempio `ANACLIENTI/...` diventa la tabella Glue `anaclienti`.
3. Riconosce le cartelle Hive `chiave=valore`, come `ingestion_date=2026-09-01`.
4. Scarica un file Parquet campione per dataset e legge il suo schema.
5. Crea la tabella Glue, oppure la aggiorna se esiste già.
6. Registra solo le partizioni non già presenti. Anche la lettura delle partizioni Glue è paginata.

Esempio:

```text
ANACLIENTI/ingestion_date=2026-09-01/batchid=123/chunk-000.parquet
```

crea/aggiorna `bronze_dev.anaclienti`, con partition keys `ingestion_date`, `batchid` e valori `2026-09-01`, `123`.

## Installazione su EC2

È richiesto Node.js 20 o superiore.

```bash
cd custom-crawler
npm install

export AWS_REGION=eu-central-1
export S3_BUCKET=lakehouse-bronze-dev-490874680158
export S3_PREFIX=""
export GLUE_DATABASE=bronze_dev

npm start
```

`S3_PREFIX` è facoltativo: vuoto esegue la scansione dell'intero bucket; `erp/` esegue la scansione di `erp/*`.

Non configurare `AWS_ACCESS_KEY_ID` o `AWS_SECRET_ACCESS_KEY`: l'AWS SDK usa automaticamente l'Instance Profile associato a EC2.

## IAM Role EC2

Al ruolo dell'istanza servono almeno:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::lakehouse-bronze-dev-490874680158"
    },
    {
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::lakehouse-bronze-dev-490874680158/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetTable",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:GetPartitions",
        "glue:GetPartition",
        "glue:CreatePartition"
      ],
      "Resource": "*"
    }
  ]
}
```

Se usate Lake Formation, concedi allo stesso ruolo anche i permessi Lake Formation sul database, sulle tabelle e sul percorso S3.

## Limiti intenzionali della prima versione

- Lo schema è letto dal primo file Parquet di ogni dataset: i file dello stesso dataset devono avere uno schema compatibile.
- I tipi primitivi comuni sono convertiti per Glue/Athena; colonne Parquet annidate sono registrate come `string` per mantenere l'esempio leggibile.
- Le partizioni non vengono cancellate quando un file S3 scompare: è un'operazione separata e potenzialmente distruttiva.
- Il file Parquet campione viene scaricato integralmente. In una versione ottimizzata si possono leggere solo i metadata/footer tramite S3 Range requests.
