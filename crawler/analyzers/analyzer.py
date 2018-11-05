from abc import ABC, abstractmethod


class Analyzer(ABC):
    @abstractmethod
    def analyze(self, share, daily_history, today_history):
        raise NotImplementedError()
