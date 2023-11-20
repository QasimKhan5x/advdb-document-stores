import json
import random

# total number of objects is 1984049
scale_factors = [1000, 496762, 992524, 1488286, 1984049]
random.seed(0)


def create_sf_data(scale_factor):
    num_objects = scale_factors[scale_factor - 1]
    total_objects = 1984049  # total number of objects in twitter.json

    # Generate a set of unique random indices to sample
    indices_to_sample = set(random.sample(range(total_objects), num_objects))

    with open("twitter.json", "r", encoding="utf-8") as file, open(
        f"./data/twitter_sf_{scale_factor}.json", "w", encoding="utf-8"
    ) as outfile:

        for i, line in enumerate(file):
            if i in indices_to_sample:
                try:
                    # Write the object to the file
                    outfile.write(line.strip())
                    outfile.write("\n")  # New line after each object

                except json.JSONDecodeError:
                    print(f"Error decoding JSON on line {i}")

                # Remove the index from the set for efficiency
                indices_to_sample.remove(i)

                # Break if we have sampled enough
                if not indices_to_sample:
                    break

# Example usage
create_sf_data(4)

# use this to load and measure execution time
# Measure-Command { mongoimport --db advdb_project --collection sf1 --file data/twitter_sf_1.json }