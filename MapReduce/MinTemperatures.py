from mrjob.job import MRJob

class MRMinTemperature(MRJob):

    def MakeFahrenheit(self, tenthsOfCelsius):
        celsius = float(tenthsOfCelsius) / 10.0
        fahrenheit = celsius * 1.8 + 32.0
        return fahrenheit

    def mapper(self, _, line):
        (location, date, type, data, x, y, z, w) = line.split(',')
        if (type == 'TMIN'):
            temperature = self.MakeFahrenheit(data)
            yield location, temperature

        # take data lines and extract location and temp

    def reducer(self, location, temps):
        yield location, min(temps)
        # report  location and the lowest temp among them

if __name__ == '__main__':
    MRMinTemperature.run()
