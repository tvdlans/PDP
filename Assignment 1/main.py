from mrjob.step import MRStep
from mrjob.job import MRJob


class MovieRatings(MRJob):

    # chaining the steps together, the second one takes the output of the first one
    # we map the output from the given file to get all movie IDs and all ratings they have
    # then we reduce it to sort by the ratings
    def steps(self):
        return [
            MRStep(mapper=self.get_movies, combiner=self.combine_movies,
                   reducer=self.count_ratings),
            MRStep(reducer=self.sort_by_rating)
        ]

    # For the 6

    # Get all the movies from the file that is provided and yield the movieID
    def get_movies(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    # Combine (join) the ratings and the id from the movie
    def combine_movies(self, rating, counts):
        yield rating, sum(counts)

    # Count the ratings per movieID
    def count_ratings(self, key, values):
        yield None, (sum(values), key)

    # For the 8

    # Sort movieID by rating
    def sort_by_rating(self, _, rating_counts):
        for count, key in sorted(rating_counts, reverse=True):
            yield (key, int(count))

    # For the 10

    #       __
    #   ___( o)>
    #   \ <_. )
    #    `---'


if __name__ == '__main__':
    MovieRatings.run()
