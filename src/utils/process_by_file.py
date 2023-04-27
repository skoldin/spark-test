import os
import shutil


def process_by_file(
        spark,
        schema,
        raw_data_path,
        valid_data_path,
        invalid_data_path,
        processed_files_dir,
        data_processor,
        file_mask='.csv'
):
    for file in os.listdir(raw_data_path):
        file_path = os.path.join(raw_data_path, file)
        if file.endswith(file_mask):
            input_df = spark.read \
                .option('header', 'true') \
                .schema(schema) \
                .csv(file_path)

            data_processor.process_data(input_df, valid_data_path, invalid_data_path)

            shutil.move(file_path, os.path.join(processed_files_dir, file))
