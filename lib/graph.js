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
    console.log(`Failed to append JSON to file <${path}>.`);
    throw e;
  }
}

// to-remove-triples.ttl
export async function getFileByNameAndApplyByBatch(
  fileName,
  task,
  callback = async () => { }
) {
  //This is still based on the filename "to-remove-triples.ttl"! This should
  //change in the future, but there is no other way to correctly address that
  //file only yet, besides via its filename.
  const queryInputContainer = `
    ${PREFIXES}
    SELECT DISTINCT ?path WHERE {
      ${rst.termToString(task.task)}
        a task:Task ;
        task:inputContainer ?inputContainer .

      ?inputContainer
        a nfo:DataContainer ;
        task:hasFile ?logicalFile .

      ?logicalFile
        a nfo:FileDataObject ;
        nfo:fileName "${fileName}" .

      ?path
        a nfo:FileDataObject ;
        nie:dataSource ?logicalFile .
    }
    LIMIT 1
  `;
  await fetchFileInputContainerAndApplyBatch(queryInputContainer, callback);
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
  const sizeBuf = process.env.BUFFER_SIZE || 10;
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
  fileName,
  task,
  callback = async () => { }
) {
  await getFileByNameAndApplyByBatch(
    fileName, task, callback
  );
}
class ArrayReadableStream extends Readable {
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
