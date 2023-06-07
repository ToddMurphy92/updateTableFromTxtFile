const mysql = require('mysql')
const mysqldump = require('mysqldump')
const Importer = require('mysql-import')
const fs = require("fs")
const _ = require('lodash')
const { exit } = require('process')


// This script updates the `database.table` table by running
// commands provided via a txt file.
// Location of the data is an argument to the script.


const dbUrl = process.env.DB_URL
const dbUser = process.env.DB_USER
const dbPassword = process.env.DB_USER_PWD
const database = process.env.DATABASE 
const table = process.env.TABLE
const txtFilePath = process.env.TXT_FILE 


const connection = mysql.createConnection({
   host: dbUrl,
   user: dbUser,
   password: dbPassword,
   database: database
});


const backupTable = async (database, dbUser, dbPassword, dbUrl) => {
   let datetime = new Date()
   let newFile = `./table_backup_${datetime}.sql`


   console.log("Next we do the mysql dump!")


   await mysqldump({
       connection: {
           host: dbUrl,
           user: dbUser,
           password: dbPassword,
           database: database,           
       },
       dump: {
           schema: {
               table: {
                   dropIfExist: true,
               },
           },
       },
       dumpToFile: newFile,
   })


   console.log("mysql dump done!")


   // Previously this statement would only run if (!testBackupFile(newFile)) which would compare
   // file sizes to check if the backup was successful but it turns out the dump and the table
   // size are off anyway by a few GB even when it works so we need to find a better way to test.
   // See testBackupFile comments for more details.
   if (!testBackupFile(newFile)) {
       console.log("Backup failed!")
       return ''// return NULL if failed
   }
   console.log("Backup success!")
   return newFile // On success return backup file name


}


const testBackupFile = async (newFile) => { // Tests the backup file  
   // to-do: Re-write and fix this function. The logic is wrong.
   // Need to re-write this function to check if backup was successful by
   // reading the last line of newFile to see if dump completed successfully
   // The last line of a successful dump will look something like this:
   // "-- Dump completed on 2020-06-17  5:04:32"
   // We need to find a way to:
   // A) read the last line B) confirm the static test exists and C) (optional) confirm date today


   const getFilesizeInMB = (filename) => { // Gets filesize in MB as a whole integer
       const fileSizeInMB = _.toInteger(_.divide(fs.statSync(filename), 1048576)) // get size in bytes and convert to MB
       return fileSizeInMB
   }


   let newFileSize = getFilesizeInMB(newFile)
   console.log('The backup file size (MB) is: ', newFileSize)


   const tableSize = async () => {
       let queryString =
           `
       SELECT           
           ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024) AS \`Size (MB)\`
       FROM
           information_schema.TABLES
       WHERE
           TABLE_SCHEMA = "${database}" AND TABLE_NAME = "${table}"
       ORDER BY
           (DATA_LENGTH + INDEX_LENGTH)
       DESC
       `


       await connection.query(queryString, function (error, results, fields) {
           if (error) throw error                      
           let size = results[0]["Size (MB)"]
           if (Number.isInteger(size)) {
               console.log('The original size is: ', size)
               return results[0] //file size in MB
           } else {
               console.log('ERROR: size is not an integer')
               console.log('Query result is: ', results)
               return "ERROR: SIZE IS NOT AN INT"
           }
       })
   }


   let originalSize = tableSize()
   console.log('originalSize value: ', originalSize)


   let result = () => {       
       return newFileSize === originalSize
   }
   console.log("result value: ", result())


   return await result()
}


const updateTable = async (database, table, dbUser, dbPassword, dbUrl, txtFilePath) => {


   console.log("*** Backup time!!! ***")
   let backupFileName = await backupTable(database, dbUser, dbPassword, dbUrl)
   console.log("*** Backup done I think ***")


   if (backupFileName.length > 20) { // If backup success then this will be a string roughly >20 characters       
       console.log("*** BACKUP SUCCESS: Table would update now if code was finished ***")


       try {
           // read contents of the file
           const data = fs.readFileSync(txtFilePath, 'UTF-8');


           // split the contents by new line
           const lines = data.split(/\r?\n/);


           // run a mysql query for each line
           lines.forEach((line) => {
               connection.query(line, function (error, results, fields) {
                   if (error) throw error
               })
           });
       } catch (err) {
           console.error(err);
       }
   } else { // If backup failed (backupFileName is NULL or a string < 20 characters)       
       const importer = new Importer({ dbUrl, dbUser, dbPassword, database });


       console.log("*** BACKUP FAILED: Backup will restore now ***")
       importer.import(txtFilePath).then(() => {
           let files_imported = importer.getImported();
           console.log(`${files_imported.length} SQL file(s) imported.`)
       }).catch(err => {
           console.error(err)
       })
   }


   connection.end()
}

// For testing the testBackupFile function
// const scriptTest = async () => {
//    console.log('*** START TEST ***')
//    let test = await testBackupFile('temp_testing_file.txt') //just throwing it a file I know exists, doesn't matter if it's not an sql backup
//    console.log('Test result: ', test)
//    console.log('*** END TEST ***')
// }
// scriptTest()

// Now to do the thing!
updateTable(database, table, dbUser, dbPassword, dbUrl, txtFilePath)
