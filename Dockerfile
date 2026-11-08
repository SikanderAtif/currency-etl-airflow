FROM astrocrpublic.azurecr.io/runtime:3.3-12

# ---- Image metadata ----
LABEL maintainer="Sikander Atif Arain sikanderatif07@gmail.com" \
      org.opencontainers.image.title="currency-etl-airflow" \
      org.opencontainers.image.description="Airflow ETL pipeline: Frankfurter exchange rates -> MongoDB" \
      org.opencontainers.image.version="1.0.0" \
      project="currency-etl" \
      component="airflow"