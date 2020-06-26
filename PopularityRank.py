from mrjob.job import MRJob
from mrjob.step import MRStep

class PopularityRank(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_popularity,
                   reducer=self.reducer_count_movieID)
        ]

    def mapper_get_popularity(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_movieID(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    PopularityRank.run()
