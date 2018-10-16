from abc import ABC, abstractmethod


class Analyzer(ABC):
    @abstractmethod
    def analyze(self, share):
        raise NotImplementedError()
