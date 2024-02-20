# map-reduce.py
import luigi

class InputData(luigi.ExternalTask):
    filename = luigi.Parameter()

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget(self.filename)
    
    def complete(self):
        """Hack so we don't have to create input files manually.
        Luigi will always think that this task is done, without checking for
        presence of source files.
        """
        return True
    

class Spliter(luigi.Task):
    filename = luigi.Parameter()
    mapperNum = luigi.IntParameter()
    nb_of_mapper = luigi.IntParameter()

    def requires(self):
        return InputData(filename=self.filename)

    def output(self):
        return luigi.LocalTarget('/app/data/splited/split_output_{}.txt'.format(self.mapperNum))

    def run(self):
        self.output().makedirs()
        with self.input().open('r') as infile, self.output().open('w') as outfile:
            lines = infile.readlines()
            chunk_size = len(lines) // self.nb_of_mapper
            start_index = self.mapperNum * chunk_size
            end_index = start_index + chunk_size
            chunk = lines[start_index:end_index]
            for line in chunk:
                outfile.write(line)


class Mapper(luigi.Task):
    mapperNum = luigi.IntParameter()
    filename = luigi.Parameter()
    nb_of_mapper = luigi.IntParameter()

    def requires(self):
        return Spliter(mapperNum=self.mapperNum, nb_of_mapper=self.nb_of_mapper, filename=self.filename)

    def output(self):
        return luigi.LocalTarget('/app/data/maped/mapped_output_{}.txt'.format(self.mapperNum))

    def map_function(self, line):
        words = line.strip().split()
        for word in words:
            yield word, 1

    def run(self):
            self.output().makedirs()
            with self.input().open('r') as infile, self.output().open('w') as outfile:
                for line in infile:
                    mapped_pairs = self.map_function(line)
                    for key, value in mapped_pairs:
                        outfile.write('{}\t{}\n'.format(key, value))


class Shuffler(luigi.Task):
    nb_of_mapper = luigi.IntParameter()
    filename = luigi.Parameter()
    reducerNum = luigi.IntParameter()
    nb_of_reducer = luigi.IntParameter()

    def requires(self):
        for mapperNum in range(self.nb_of_mapper):
            yield Mapper(mapperNum=mapperNum, filename=self.filename,nb_of_mapper=self.nb_of_mapper)

    def output(self):
        return luigi.LocalTarget('/app/data/shuffled/shuffled_output_{}.txt'.format(self.reducerNum))

    def run(self):
        self.output().makedirs()
        with self.output().open('w') as outfile:
            for input in self.input():
                with input.open('r') as infile:
                    for line in infile:
                        # Determine which file to write to based on the first letter
                        first_letter = line[0].lower()
                        index = (ord(first_letter) - ord('a')) % self.nb_of_reducer
                        if index == self.reducerNum:
                            outfile.write(line)


class Reducer(luigi.Task):
    reducerNum = luigi.IntParameter()
    nb_of_mapper = luigi.IntParameter()
    nb_of_reducer = luigi.IntParameter()
    filename = luigi.Parameter()

    def requires(self):
        return Shuffler(nb_of_reducer=self.nb_of_reducer,reducerNum=self.reducerNum, nb_of_mapper=self.nb_of_mapper, filename=self.filename)

    def output(self):
        return luigi.LocalTarget('/app/data/reduced/reducer_output_{}.txt'.format(self.reducerNum))

    def run(self):
        word_count = {}
        with self.input().open('r') as infile:
            for line in infile:
                words = line.split()
                for word in words:
                    word = word.strip()  # Remove any leading or trailing whitespace
                    word_count[word] = word_count.get(word, 0) + 1

        with self.output().open('w') as outfile:
            for word, count in word_count.items():
                outfile.write(f"{word}: {count}\n")


class BundleReducerOutput(KubernetesJobTask):
    nb_of_reducer = luigi.IntParameter()
    nb_of_mapper = luigi.IntParameter()
    filename = luigi.Parameter()
    
    def requires(self):
        for reducerNum in range(self.nb_of_reducer):
            yield Reducer(nb_of_reducer=self.nb_of_reducer,reducerNum=reducerNum, nb_of_mapper=self.nb_of_mapper, filename=self.filename)

    def output(self):
        return luigi.LocalTarget('/app/data/bundled_output.txt')

    def run(self):
        with self.output().open('w') as outfile:
            for input in self.input():
                with input.open('r') as infile:
                    for line in infile:
                        outfile.write(line)


if __name__ == '__main__':
    luigi.build([BundleReducerOutput(nb_of_reducer=3, nb_of_mapper=4, filename='input.txt')], local_scheduler=True)