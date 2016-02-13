Undocumented Python objects
===========================
dbprocessing.DBStrings
----------------------
Classes:
 * DBFormatter -- missing methods:

   - assemble
   - expand_datetime
   - expand_format
   - format
   - re

dbprocessing.DBUtils
--------------------
Classes:
 * DBUtils -- missing methods:

   - addCode
   - addFile
   - addFilecodelink
   - addFilefilelink
   - addInspector
   - addInstrument
   - addInstrumentproductlink
   - addMission
   - addProcess
   - addProduct
   - addRelease
   - addSatellite
   - addproductprocesslink
   - checkFileSHA
   - checkFiles
   - delFilecodelink
   - delFilefilelink
   - delInspector
   - file_id_Clean
   - getActiveInspectors
   - getAllCodes
   - getAllCodesFromProcess
   - getAllFileIds
   - getAllFilenames
   - getAllInstruments
   - getAllProcesses
   - getAllProducts
   - getAllSatellites
   - getChildrenProcesses
   - getCodeFromProcess
   - getCodeID
   - getCodePath
   - getCodeVersion
   - getEntry
   - getErrorPath
   - getFileDates
   - getFileFullPath
   - getFileID
   - getFileParents
   - getFilecodelink_byfile
   - getFilesByCode
   - getFilesByDate
   - getFilesByInstrument
   - getFilesByLevel
   - getFilesByProduct
   - getFilesByProductDate
   - getIncomingPath
   - getInputProductID
   - getInstrumentID
   - getMissionDirectory
   - getMissionID
   - getMissions
   - getProcessFromInputProduct
   - getProcessFromOutputProduct
   - getProcessID
   - getProcessTimebase
   - getProductID
   - getProductParentTree
   - getProductsByInstrument
   - getProductsByLevel
   - getRunProcess
   - getSatelliteID
   - getSatelliteMission
   - getTraceback
   - getVersion
   - list_release
   - renameFile
   - tag_release
   - updateInspectorSubs
   - updateProcessSubs
   - updateProductSubs

dbprocessing.DBfile
-------------------
Classes:
 * DBfile -- missing methods:

   - addFileToDB
   - checkVersion
   - getDirectory
   - move

dbprocessing.DBlogging
----------------------
Functions:
 * change_logfile

dbprocessing.DBqueue
--------------------
Classes:
 * DBqueue -- missing methods:

   - popiter
   - popleftiter

dbprocessing.Diskfile
---------------------
Classes:
 * DigestError
 * Diskfile -- missing methods:

   - checkAccess
 * FilenameError
 * InputError
 * ReadError
 * WriteError

dbprocessing.dbprocessing
-------------------------
Classes:
 * ProcessQueue -- missing methods:

   - buildChildren
   - checkIncoming
   - diskfileToDB
   - figureProduct
   - importFromIncoming
   - mk_tempdir
   - moveToError
   - onStartup
   - reprocessByAll
   - reprocessByCode
   - reprocessByDate
   - reprocessByInstrument
   - reprocessByProduct
   - rm_tempdir

dbprocessing.module
-------------------
Classes:
 * module -- missing methods:

   - get_env

dbprocessing.reports
--------------------
Classes:
 * commandsRun -- missing methods:

   - html
   - htmlheader
 * errors -- missing methods:

   - html
   - htmlheader
 * ingested -- missing methods:

   - html
   - htmlheader
 * logfile -- missing methods:

   - setTimerange
 * movedToError -- missing methods:

   - html
   - htmlheader

dbprocessing.runMe
------------------
Classes:
 * runMe -- missing methods:

   - make_command_line
   - moveToError
   - moveToIncoming
 * runObj

