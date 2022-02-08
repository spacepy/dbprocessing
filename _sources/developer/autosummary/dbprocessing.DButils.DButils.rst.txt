dbprocessing.DButils.DButils
============================

.. currentmodule:: dbprocessing.DButils

.. autoclass:: DButils

   
   .. automethod:: __init__

   
   .. rubric:: Methods

   .. autosummary::
   
   
      ~DButils.ProcessqueueClean
   
   
      ~DButils.ProcessqueueFlush
   
   
      ~DButils.ProcessqueueGet
   
   
      ~DButils.ProcessqueueGetAll
   
   
      ~DButils.ProcessqueueLen
   
   
      ~DButils.ProcessqueuePop
   
   
      ~DButils.ProcessqueuePush
   
   
      ~DButils.ProcessqueueRawadd
   
   
      ~DButils.ProcessqueueRemove
   
   
   
      ~DButils.addCode
   
   
      ~DButils.addFile
   
   
      ~DButils.addFilecodelink
   
   
      ~DButils.addFilefilelink
   
   
      ~DButils.addInspector
   
   
      ~DButils.addInstrument
   
   
      ~DButils.addInstrumentproductlink
   
   
      ~DButils.addLogging
   
   
      ~DButils.addMission
   
   
      ~DButils.addProcess
   
   
      ~DButils.addProduct
   
   
      ~DButils.addRelease
   
   
      ~DButils.addSatellite
   
   
      ~DButils.addUnixTimeTable
   
   
      ~DButils.addproductprocesslink
   
   
      ~DButils.checkDiskForFile
   
   
      ~DButils.checkFileSHA
   
   
      ~DButils.checkFiles
   
   
      ~DButils.checkIncoming
   
   
      ~DButils.closeDB
   
   
      ~DButils.codeIsActive
   
   
      ~DButils.commitDB
   
   
      ~DButils.currentlyProcessing
   
   
      ~DButils.delFilecodelink
   
   
      ~DButils.delFilefilelink
   
   
      ~DButils.delInspector
   
   
      ~DButils.delProduct
   
   
      ~DButils.delProductProcessLink
   
   
      ~DButils.editTable
   
   
      ~DButils.fileIsNewest
   
   
      ~DButils.file_id_Clean
   
   
      ~DButils.getActiveInspectors
   
   
      ~DButils.getAllCodes
   
   
      ~DButils.getAllCodesFromProcess
   
   
      ~DButils.getAllFileIds
   
   
      ~DButils.getAllFilenames
   
   
      ~DButils.getAllInstruments
   
   
      ~DButils.getAllProcesses
   
   
      ~DButils.getAllProducts
   
   
      ~DButils.getAllSatellites
   
   
      ~DButils.getChildTree
   
   
      ~DButils.getChildrenProcesses
   
   
      ~DButils.getCodeDirectory
   
   
      ~DButils.getCodeFromProcess
   
   
      ~DButils.getCodeID
   
   
      ~DButils.getCodePath
   
   
      ~DButils.getCodeVersion
   
   
      ~DButils.getDirectory
   
   
      ~DButils.getEntry
   
   
      ~DButils.getErrorPath
   
   
      ~DButils.getFileDates
   
   
      ~DButils.getFileFullPath
   
   
      ~DButils.getFileID
   
   
      ~DButils.getFileParents
   
   
      ~DButils.getFileVersion
   
   
      ~DButils.getFilecodelink_bycode
   
   
      ~DButils.getFilecodelink_byfile
   
   
      ~DButils.getFiles
   
   
      ~DButils.getFilesByCode
   
   
      ~DButils.getFilesByDate
   
   
      ~DButils.getFilesByInstrument
   
   
      ~DButils.getFilesByProduct
   
   
      ~DButils.getFilesByProductDate
   
   
      ~DButils.getFilesByProductTime
   
   
      ~DButils.getIncomingPath
   
   
      ~DButils.getInputProductID
   
   
      ~DButils.getInspectorDirectory
   
   
      ~DButils.getInstrumentID
   
   
      ~DButils.getMissionDirectory
   
   
      ~DButils.getMissionID
   
   
      ~DButils.getMissions
   
   
      ~DButils.getProcessFromInputProduct
   
   
      ~DButils.getProcessFromOutputProduct
   
   
      ~DButils.getProcessID
   
   
      ~DButils.getProcessTimebase
   
   
      ~DButils.getProductID
   
   
      ~DButils.getProductParentTree
   
   
      ~DButils.getProductsByInstrument
   
   
      ~DButils.getProductsByLevel
   
   
      ~DButils.getRunProcess
   
   
      ~DButils.getSatelliteID
   
   
      ~DButils.getSatelliteMission
   
   
      ~DButils.getTraceback
   
   
      ~DButils.list_release
   
   
      ~DButils.openDB
   
   
      ~DButils.purgeProcess
   
   
      ~DButils.renameFile
   
   
      ~DButils.resetProcessingFlag
   
   
      ~DButils.startLogging
   
   
      ~DButils.stopLogging
   
   
      ~DButils.tag_release
   
   
      ~DButils.updateCodeNewestVersion
   
   
      ~DButils.updateInspectorSubs
   
   
      ~DButils.updateProcessSubs
   
   
      ~DButils.updateProductSubs
   
   
   

   
   
   

   
   
   
   
   .. automethod::  ProcessqueueClean
   
   
   .. automethod::  ProcessqueueFlush
   
   
   .. automethod::  ProcessqueueGet
   
   
   .. automethod::  ProcessqueueGetAll
   
   
   .. automethod::  ProcessqueueLen
   
   
   .. automethod::  ProcessqueuePop
   
   
   .. automethod::  ProcessqueuePush
   
   
   .. automethod::  ProcessqueueRawadd
   
   
   .. automethod::  ProcessqueueRemove
   
   
   
   .. automethod::  addCode
   
   
   .. automethod::  addFile
   
   
   .. automethod::  addFilecodelink
   
   
   .. automethod::  addFilefilelink
   
   
   .. automethod::  addInspector
   
   
   .. automethod::  addInstrument
   
   
   .. automethod::  addInstrumentproductlink
   
   
   .. automethod::  addLogging
   
   
   .. automethod::  addMission
   
   
   .. automethod::  addProcess
   
   
   .. automethod::  addProduct
   
   
   .. automethod::  addRelease
   
   
   .. automethod::  addSatellite
   
   
   .. automethod::  addUnixTimeTable
   
   
   .. automethod::  addproductprocesslink
   
   
   .. automethod::  checkDiskForFile
   
   
   .. automethod::  checkFileSHA
   
   
   .. automethod::  checkFiles
   
   
   .. automethod::  checkIncoming
   
   
   .. automethod::  closeDB
   
   
   .. automethod::  codeIsActive
   
   
   .. automethod::  commitDB
   
   
   .. automethod::  currentlyProcessing
   
   
   .. automethod::  delFilecodelink
   
   
   .. automethod::  delFilefilelink
   
   
   .. automethod::  delInspector
   
   
   .. automethod::  delProduct
   
   
   .. automethod::  delProductProcessLink
   
   
   .. automethod::  editTable
   
   
   .. automethod::  fileIsNewest
   
   
   .. automethod::  file_id_Clean
   
   
   .. automethod::  getActiveInspectors
   
   
   .. automethod::  getAllCodes
   
   
   .. automethod::  getAllCodesFromProcess
   
   
   .. automethod::  getAllFileIds
   
   
   .. automethod::  getAllFilenames
   
   
   .. automethod::  getAllInstruments
   
   
   .. automethod::  getAllProcesses
   
   
   .. automethod::  getAllProducts
   
   
   .. automethod::  getAllSatellites
   
   
   .. automethod::  getChildTree
   
   
   .. automethod::  getChildrenProcesses
   
   
   .. automethod::  getCodeDirectory
   
   
   .. automethod::  getCodeFromProcess
   
   
   .. automethod::  getCodeID
   
   
   .. automethod::  getCodePath
   
   
   .. automethod::  getCodeVersion
   
   
   .. automethod::  getDirectory
   
   
   .. automethod::  getEntry
   
   
   .. automethod::  getErrorPath
   
   
   .. automethod::  getFileDates
   
   
   .. automethod::  getFileFullPath
   
   
   .. automethod::  getFileID
   
   
   .. automethod::  getFileParents
   
   
   .. automethod::  getFileVersion
   
   
   .. automethod::  getFilecodelink_bycode
   
   
   .. automethod::  getFilecodelink_byfile
   
   
   .. automethod::  getFiles
   
   
   .. automethod::  getFilesByCode
   
   
   .. automethod::  getFilesByDate
   
   
   .. automethod::  getFilesByInstrument
   
   
   .. automethod::  getFilesByProduct
   
   
   .. automethod::  getFilesByProductDate
   
   
   .. automethod::  getFilesByProductTime
   
   
   .. automethod::  getIncomingPath
   
   
   .. automethod::  getInputProductID
   
   
   .. automethod::  getInspectorDirectory
   
   
   .. automethod::  getInstrumentID
   
   
   .. automethod::  getMissionDirectory
   
   
   .. automethod::  getMissionID
   
   
   .. automethod::  getMissions
   
   
   .. automethod::  getProcessFromInputProduct
   
   
   .. automethod::  getProcessFromOutputProduct
   
   
   .. automethod::  getProcessID
   
   
   .. automethod::  getProcessTimebase
   
   
   .. automethod::  getProductID
   
   
   .. automethod::  getProductParentTree
   
   
   .. automethod::  getProductsByInstrument
   
   
   .. automethod::  getProductsByLevel
   
   
   .. automethod::  getRunProcess
   
   
   .. automethod::  getSatelliteID
   
   
   .. automethod::  getSatelliteMission
   
   
   .. automethod::  getTraceback
   
   
   .. automethod::  list_release
   
   
   .. automethod::  openDB
   
   
   .. automethod::  purgeProcess
   
   
   .. automethod::  renameFile
   
   
   .. automethod::  resetProcessingFlag
   
   
   .. automethod::  startLogging
   
   
   .. automethod::  stopLogging
   
   
   .. automethod::  tag_release
   
   
   .. automethod::  updateCodeNewestVersion
   
   
   .. automethod::  updateInspectorSubs
   
   
   .. automethod::  updateProcessSubs
   
   
   .. automethod::  updateProductSubs
   
   
   

   
   
   