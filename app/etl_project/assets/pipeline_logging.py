import logging
import time


class PipelineLogging:
    def __init__(self, pipeline_name: str, log_folder_path: str):
        self.pipeline_name = pipeline_name
        self.log_folder_path = log_folder_path

        # Initialize logger
        logger = logging.getLogger(pipeline_name)
        logger.setLevel(logging.INFO)

        # Create log file path
        current_time = time.strftime("%Y-%m-%d_%H-%M-%S")
        self.file_path = (
            f"{self.log_folder_path}/{self.pipeline_name}_{current_time}.log"
        )

        # Create handlers
        file_handler = logging.FileHandler(self.file_path)
        file_handler.setLevel(logging.INFO)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)

        # Create formatters and add them to the handlers
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)

        # Add handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

        self.logger = logger

    def get_logs(self) -> str:
        with open(self.file_path, "r") as file:
            return "".join(file.readlines())


# Example usage:
# logger, log_file = setup_pipeline_logging("pipeline_name", "/path/to/logs")
# logger.info("This is an info log message.")
# logs = get_logs(log_file)
# print(logs)
