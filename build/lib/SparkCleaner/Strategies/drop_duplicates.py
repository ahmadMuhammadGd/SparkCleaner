from SparkCleaner.Strategies.base import CleaningStrategy, DataFrame

class DropDuplicatesStrategy(CleaningStrategy):
    def clean(self, df: DataFrame) -> DataFrame:
        columns_to_select = [col for col in self.columns if col != "__index"]
        initial_count = df.count()
        df_cleaned = df.dropDuplicates(subset=columns_to_select)
        final_count = df_cleaned.count()
        
        if initial_count > final_count:
            self.logger.log_error("Duplicate Removal", "Data Cleaning", f"Dropped {initial_count - final_count} duplicate rows.")
        
        return df_cleaned