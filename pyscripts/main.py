from hr_data_processor import HRDataProcessor


def process_hr_data():
    hr_data_processor = HRDataProcessor()
    # hr_data_processor.create_database_objects()
    # hr_data_processor.ingest_new_files()
    # hr_data_processor.load_data_into_database()
    hr_data_processor.load_historical_data()


process_hr_data()