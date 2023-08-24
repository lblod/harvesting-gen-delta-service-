# delta-generation-service

Generate delta files based on `new-insert-triples.ttl` and `to-remove-triples.ttl` file produced by the diff service.
Delete's and Insert's are in separate files (we don't merge both for performance reason).
We first produce delete delta files, so that consumers can first delete outdated triples before inserting new ones.

## Usage

```yml
  harvest_gen_delta:
   image: lblod/delta-generation-service
   environment:
     BUFFER_SIZE: "100"
   volumes:
     - ./data/files:/share
```

## environment variables:

`BUFFER_SIZE: "10"`
`TARGET_DIRECTORY_DELTA_PATH: "/share/delta-generation"`


