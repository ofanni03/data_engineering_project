import logging
from pathlib import Path
import config


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)-9s %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)


def get_spark(app_name="tourism-pipeline"):
    """Start PySpark in local mode (all CPU cores of this machine)."""
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")  # small data -> few partitions
        .config("spark.ui.showConsoleProgress", "false")
    )
    if config.USE_AZURE:
        # The hadoop-azure package lets Spark read/write abfss:// paths.
        # 3.3.4 = the Hadoop version bundled with PySpark 3.5.
        builder = builder.config(
            "spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4"
        ).config(
            f"spark.hadoop.fs.azure.account.key.{config.AZURE_ACCOUNT}.dfs.core.windows.net",
            config.AZURE_KEY,
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def save_raw_file(relative_path, content):
    #Save one raw file unchanged,overwrites the same path on re-run
    #Locally: data/raw/<relative_path>
    #Azure:   <container>/raw/<relative_path> 
    
    if config.USE_AZURE:
        from azure.storage.blob import BlobServiceClient

        service = BlobServiceClient(
            account_url=f"https://{config.AZURE_ACCOUNT}.blob.core.windows.net",
            credential=config.AZURE_KEY,
        )
        blob = service.get_blob_client(config.AZURE_CONTAINER, f"raw/{relative_path}")
        blob.upload_blob(content, overwrite=True)
        return f"{config.RAW}/{relative_path}"

    path = Path(config.RAW) / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return str(path)