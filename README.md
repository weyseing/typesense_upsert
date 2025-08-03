# Setup
- Copy `.env.example` to `.env` and fill up for below.
    - `TYPESENSE_ENDPOINT`
- **Start up docker** via `docker compose up -d`
- Point **Sink-HTTP connector** to **[POST request]** `http://typesense_upsert/typesense/transaction` to transfer data to Typesense.






