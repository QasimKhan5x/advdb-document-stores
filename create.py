import json
import random

# total number of objects is 1984049
scale_factors = [1000, 496762, 992524, 1488286, 1984049]
random.seed(0)


def create_sf_data(scale_factor):
    num_objects = scale_factors[scale_factor - 1]
    total_objects = 1984049
    indices_to_sample = set(random.sample(range(total_objects), num_objects))

    with open("twitter.json", "r", encoding="utf-8") as file, open(
        f"data/twitter_sf_{scale_factor}_2.json", "w", encoding="utf-8"
    ) as outfile:
        # Start the JSON array
        outfile.write('{"docs": [\n')
        first_object = True
        for i, line in enumerate(file):
            if i in indices_to_sample:
                try:
                    json_object = json.loads(line.strip())
                    oid = json_object["_id"]["$oid"]
                    json_object["_id"] = oid
                    if not first_object:
                        outfile.write(
                            ",\n"
                        )  # Add comma before the next object (except the first)
                    json.dump(json_object, outfile)  # Write the object to the file
                    first_object = False  # Update flag after writing the first object
                except json.JSONDecodeError:
                    print(f"Error decoding JSON on line {i}")
                indices_to_sample.remove(i)
                if not indices_to_sample:
                    break
        # Close the JSON array
        outfile.write("\n]}")


# Example usage
create_sf_data(1)
