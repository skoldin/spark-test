from abc import ABC, abstractmethod


class Processor(ABC):

    @abstractmethod
    def process_data(self, input_df, valid_output_dir, invalid_output_dir):
        pass
