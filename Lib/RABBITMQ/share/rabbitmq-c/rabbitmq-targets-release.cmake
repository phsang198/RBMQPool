#----------------------------------------------------------------
# Generated CMake target import file for configuration "Release".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "rabbitmq::rabbitmq" for configuration "Release"
set_property(TARGET rabbitmq::rabbitmq APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(rabbitmq::rabbitmq PROPERTIES
  IMPORTED_IMPLIB_RELEASE "${_IMPORT_PREFIX}/lib/rabbitmq.4.lib"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/bin/rabbitmq.4.dll"
  )

list(APPEND _cmake_import_check_targets rabbitmq::rabbitmq )
list(APPEND _cmake_import_check_files_for_rabbitmq::rabbitmq "${_IMPORT_PREFIX}/lib/rabbitmq.4.lib" "${_IMPORT_PREFIX}/bin/rabbitmq.4.dll" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
