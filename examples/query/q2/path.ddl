Create table Graph (
    src INT,
    dst INT,
    weight INT
) WITH (
    'path' = 'examples/data/path.dat'
)