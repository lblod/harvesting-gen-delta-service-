import { sparqlEscapeUri, } from 'mu';
import { appendFile, } from 'fs/promises';
import { createReadStream } from "fs";
import * as sjp from 'sparqljson-parse';
import { querySudo as query } from '@lblod/mu-auth-sudo';
import { PREFIXES } from '../constants';
import readline from 'readline';
import { Readable } from 'stream';

import * as stream from 'stream';
import * as N3 from 'n3';
export async function getFilePath(remoteFileUri) {
  const result = await query(`
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

    SELECT ?file
    WHERE {
      ?file nie:dataSource ${sparqlEscapeUri(remoteFileUri)} .
    } LIMIT 1`);
  if (result.results.bindings.length) {
    const file = result.results.bindings[0]['file'].value;
    console.log(`Getting contents of file ${file}`);
    const path = file.replace('share://', '/share/');
    return path;
  }
}


export async function appendJsonFile(content, path) {
  try {
    await appendFile(path, content, 'utf-8');
  } catch (e) {
    console.log(`Failed to append TTL to file <${path}>.`);
    throw e;
  }
}

export async function fetchFileInputContainerAndApplyBatch(
  queryInputContainer,
  callback
) {
  const fileResponse = await query(queryInputContainer);
  const parser = new sjp.SparqlJsonParser();
  const file = parser.parseJsonResults(fileResponse)[0].path;
  const path = file.value.replace('share://', '/share/');

  const fileStream = createReadStream(path);
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  let buf = [];
  const sizeBuf = 100;
  for await (const line of rl) {
    buf.push(line); // assuming N-TRIPLES notation
    if (buf.length == sizeBuf) {
      const arrayStream = new ArrayReadableStream(buf);
      const store = await streamToN3Store(arrayStream);
      await callback(store);
      buf = [];
    }
  }
  if (buf.length) {
    const arrayStream = new ArrayReadableStream(buf);
    const store = await streamToN3Store(arrayStream);
    await callback(store);
    buf = [];
  }
}
export async function getTriplesInFileAndApplyByBatch(
  task,
  callback = async () => { }
) {
  await fetchFileInputContainerAndApplyBatch(
    `
    ${PREFIXES}
    SELECT DISTINCT ?path WHERE {
      GRAPH ?g {
        BIND(${sparqlEscapeUri(task.task)} as ?task).
        ?task task:inputContainer ?container.
        ?container task:hasGraph ?graph.
        ?graph task:hasFile ?file.
        ?path nie:dataSource ?file.
      }
    }
    LIMIT 1
  `,
    callback
  );
} class ArrayReadableStream extends Readable {
  constructor(array, options = {}) {
    super(options);
    this.array = array;
    this.index = 0;
  }

  _read() {
    if (this.index >= this.array.length) {
      this.push(null);
      return;
    }

    const chunk = this.array[this.index];
    this.push(chunk);
    this.index++;
  }
}

function streamToN3Store(arrayStream) {
  const store = new N3.Store();
  const consumer = new stream.Writable({
    write(quad, _encoding, done) {
      store.addQuad(quad);
      done();
    },
    objectMode: true,
  });
  const streamParser = new N3.StreamParser();
  arrayStream.pipe(streamParser);
  streamParser.pipe(consumer);
  return new Promise((resolve, reject) => {
    consumer.on('close', () => {
      resolve(store);
    });
    consumer.on('error', reject);
  });
}
