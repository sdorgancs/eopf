import json
import sys
from traceback import print_exc

import click

from eopf.algorithms import ProcessingContext
from eopf.core.computing.pool import DistributedPool, LocalPool
from eopf.core.production.triggering import import_algorithms, registry


@click.group()
def cli():
    pass


@cli.command(help="List registered algorithms")
def list():
    for algo in registry.algorithms:
        print(algo)


schemahelp = """Prints algorithm json schema on STDOUT
\nArguments:
\n\t- ALGORITHM is the name of the algorithm
"""


@cli.command(help=schemahelp)
@click.argument("algorithm")
def schema(algorithm):

    if algorithm in registry.algorithms:
        algo = registry.algorithms[algorithm]
        d = dict()
        d["description"] = algo.__doc__
        d["input"] = algo.input_class().json_schema()
        d["output"] = algo.output_class().json_schema()
        json.dump(d, sys.stdout)
    else:
        print("Error: {algorithm} does not exits", file=sys.stderr)
        print("Use list command to find available algorithms", file=sys.stderr)
        exit(1)


describehelp = """Print the description of an algorithm on STDOUT
\nArguments:
\n\t- ALGORITHM is the name of the algorithm to describe
"""


@cli.command(help=describehelp)
@click.argument("algorithm")
def describe(algorithm):
    if algorithm in registry.algorithms:
        algo = registry.algorithms[algorithm]
        print(algo.__doc__)
    else:
        print("Error: {algorithm} does not exits", file=sys.stderr)
        print("Use list command to find available algorithms", file=sys.stderr)
        exit(1)


runhelp = """Runs an algorithm
\nArguments:
\n\t- ALGORITHM is the name of the algorithm to run, to display available algorithms one can use 'list' command
\n\t- INPUT_FILE is a json file containing algorithm input parameters
\n\t- OUTPUT_FILE is a file where the result of the algorithm is written
\nIf an error occurs during the execution of the algorithm, exit status of the command is not zero and the error analysis is written in OUTPUT_FILE.error
"""


@cli.command(help=runhelp)
@click.argument("algorithm")
@click.argument("input_file")
@click.argument("output_file")
@click.option(
    "--resources",
    default=None,
    help="the path to a json file a URL to a json file describing the resource configuration. \
        By default the algorithm is executed locally and uses the maximum available resources.",
)
def run(algorithm, input_file, output_file, use_ray):
    try:
        if algorithm in registry.algorithms:
            algo = registry.algorithms[algorithm]
            with open(input_file) as fi:
                input_class = algo.input_class()
                param = input_class.from_json(fi.read(), validate=True)
                if use_ray:
                    context = ProcessingContext(DistributedPool(), None)
                else:
                    context = ProcessingContext(LocalPool(), None)
                output = algo(context)(param)
                jsoutout = output.to_json()
                with open(output_file, "w") as fo:
                    fo.write(jsoutout)
        else:
            print(f"Error: {algorithm} does not exits", file=sys.stderr)
            print("Use list command to find available algorithms", file=sys.stderr)
            exit(1)

    except BaseException:
        print(f"An exception occurs running {algorithm} algorithm:", file=sys.stderr)
        print_exc(file=sys.stderr)
        exit(2)


if __name__ == "__main__":
    import logging

    logging.basicConfig(filename="eopf_cli.log", level=logging.INFO)
    import_algorithms()
    cli()
