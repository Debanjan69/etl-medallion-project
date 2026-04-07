from src.transformation.bronze_to_silver import transform_to_silver
from src.transformation.silver_to_gold import transform_to_gold
from src.utils.logger import get_logger

logger = get_logger()

def run_pipeline():
    try:
        logger.info("Starting Silver Transformation...")
        silver_df = transform_to_silver()

        logger.info("Silver Layer Completed")

        logger.info("Starting Gold Transformation...")
        gold_df = transform_to_gold()

        logger.info("Gold Layer Completed")

    except Exception as e:
        logger.error(f"Pipeline Failed: {str(e)}")
        raise

if __name__ == "__main__":
    run_pipeline()
