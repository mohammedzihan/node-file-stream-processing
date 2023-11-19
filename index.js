const { Storage } = require('@google-cloud/storage');
const { Pool } = require('pg');
const { Readable } = require('stream');
const storage = new Storage();
const pool = new Pool({
  // PG database connection config
});

async function processFile(bucketName, fileName) {
  const bucket = storage.bucket(bucketName);
  const file = bucket.file(fileName);
  
  const streamOptions = {
    // options such as start and end bytes can be set for each chunk
  };

  let chunkIndex = 0;
  let streamEnd = false;
  let processNextChunk = true;

  while (!streamEnd && processNextChunk) {
    // In this example, each chunk could be a range of bytes.
    streamOptions.start = chunkIndex * CHUNK_SIZE;
    streamOptions.end = (chunkIndex + 1) * CHUNK_SIZE - 1;

    const chunkStream = file.createReadStream(streamOptions)
      .on('end', () => {
        // End of the current chunk
        streamEnd = true;
      })
      .on('error', processError);
      
    processNextChunk = await processChunk(chunkStream);

    chunkIndex++;
  }
}

async function processChunk(chunkStream) {
  const dataString = await streamToString(chunkStream);

  // Your logic to parse and process data goes here

  const insertResult = await insertDataIntoPg(dataString); // Define insertDataIntoPg to insert into PostgreSQL

  return insertResult; // This should be a boolean indicating if the next chunk should be processed
}

function streamToString(stream) {
  const chunks = [];
  return new Promise((resolve, reject) => {
    stream.on('data', chunk => chunks.push(chunk));
    stream.on('error', reject);
    stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8'))); // Assumes file encoding is 'utf8'
  });
}

async function insertDataIntoPg(dataString) {
  const client = await pool.connect();
  try {
    // Split dataString into individual data items and insert into the database
    // This is dependent on the structure of your data and the schema of your PG database

    // Example of insertion query
    const insertQuery = 'INSERT INTO your_table(column1, column2) VALUES($1, $2)';
    await client.query(insertQuery, [/* values */]);
    
    return true; // returning true to indicate success
  } catch (error) {
    console.error('Failed to insert data into PG', error);
    return false; // returning false to indicate failure and stop processing further chunks
  } finally {
    client.release();
  }
}

// Error handler
function processError(error) {
  console.error('An error occurred: ', error);
  process.exit(1); // Exit or handle the error by retrying
}

// Usage
const BUCKET_NAME = 'your-bucket-name';
const FILE_NAME = 'your-large-file-name';
const CHUNK_SIZE = 64 * 1024; // 64KB, you can adjust the size of the chunk

processFile(BUCKET_NAME, FILE_NAME).then(() => {
  console.log('File processing complete');
}).catch(error => {
  console.error('File processing failed: ', error);
});