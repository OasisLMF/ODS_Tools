ODS_Tools Changelog
===================

`3.2.0`_
 ---------
* [#78](https://github.com/OasisLMF/ODS_Tools/pull/78) - Release 3.1.4
* [#80](https://github.com/OasisLMF/ODS_Tools/pull/80) - Added fields to analysis settings schema
* [#81](https://github.com/OasisLMF/ODS_Tools/pull/81) - Add vulnerability adjustments field to schema
* [#82](https://github.com/OasisLMF/ODS_Tools/pull/83) - Enable additional properties for additional assets in model_settings.json
* [#72, #55](https://github.com/OasisLMF/ODS_Tools/pull/84) - Remove warning message
* [#85](https://github.com/OasisLMF/ODS_Tools/pull/86) - Add api run_mode, model running on (v1 / v2), to model settings 
* [#88](https://github.com/OasisLMF/ODS_Tools/pull/88) - Lot3 integration with ODS-tools package
.. _`3.2.0`:  https://github.com/OasisLMF/ODS_Tools/compare/3.1.4...3.2.0

.. _`3.1.4`:  https://github.com/OasisLMF/ODS_Tools/compare/3.1.3...3.1.4

`3.1.3`_
 ---------
* [#64](https://github.com/OasisLMF/ODS_Tools/pull/66) - Backward compatibility when adding new codes in OED
* [#68](https://github.com/OasisLMF/ODS_Tools/pull/69) - Define relationships between event and footprint sets
* [#70](https://github.com/OasisLMF/ODS_Tools/pull/70) - Fix/forex case error
* [#73](https://github.com/OasisLMF/ODS_Tools/pull/73) - Feature/peril filter
.. _`3.1.3`:  https://github.com/OasisLMF/ODS_Tools/compare/3.1.2...3.1.3

`3.1.2`_
 ---------
* [#53](https://github.com/OasisLMF/ODS_Tools/pull/53) - Release 3.1.1 (staging)
* [#62](https://github.com/OasisLMF/ODS_Tools/pull/62) - Add fields for running aalcalcmeanonly ktools component
.. _`3.1.2`:  https://github.com/OasisLMF/ODS_Tools/compare/3.1.1...3.1.2

`3.1.1`_
 ---------
* [#39](https://github.com/OasisLMF/ODS_Tools/pull/39) - Release 3.1.0 - for next stable oasislmf release 
* [#44](https://github.com/OasisLMF/ODS_Tools/pull/44) - add check for conditional requirement
* [#50](https://github.com/OasisLMF/ODS_Tools/pull/50) - Update CI for stable 3.1.x
* [#52](https://github.com/OasisLMF/ODS_Tools/pull/52) - Fix/improve check perils
* [#54](https://github.com/OasisLMF/ODS_Tools/pull/54) - Add footprint file suffix options
* [#58](https://github.com/OasisLMF/ODS_Tools/pull/59) - Validation crash after converting account file from csv to parquet
* [#60](https://github.com/OasisLMF/ODS_Tools/pull/60) - Add options to enable/disable post loss amplification, and set secondary and uniform post loss amplification factors
* [#61](https://github.com/OasisLMF/ODS_Tools/pull/61) - Model_settings, allow additional properties under 'data_settings'
.. _`3.1.1`:  https://github.com/OasisLMF/ODS_Tools/compare/3.1.0...3.1.1


`3.1.0`_
 ---------
* [#33](https://github.com/OasisLMF/ODS_Tools/pull/32) - Consistant managment of blank values
* [#35](https://github.com/OasisLMF/ODS_Tools/pull/36) - Bug in Analysis settings compatibility map
* [#40](https://github.com/OasisLMF/ODS_Tools/pull/40) - Replace hardcoded packages, with find_packages
* [#41](https://github.com/OasisLMF/ODS_Tools/pull/41) - fix check category
* [#42](https://github.com/OasisLMF/ODS_Tools/pull/43) - ods_tools check crashes on empty location file
* [#44](https://github.com/OasisLMF/ODS_Tools/pull/44) - add check for conditional requirement
* [#46](https://github.com/OasisLMF/ODS_Tools/pull/46) - add option to manage unknown column when saving files
* [#27](https://github.com/OasisLMF/ODS_Tools/pull/27) - fix read_csv only read needed line when reading header
* [#30](https://github.com/OasisLMF/ODS_Tools/pull/30) - Release/3.0.6
* [#31](https://github.com/OasisLMF/ODS_Tools/pull/31) - Add Platform testing to ODS-tools github actions
.. _`3.1.0`:  https://github.com/OasisLMF/ODS_Tools/compare/3.0.7...3.1.0

`3.0.6`_
 ---------
* [#21](https://github.com/OasisLMF/ODS_Tools/pull/22) - LocPeril raise validation error when Blank but it should be allowed according to schema
* [#23](https://github.com/OasisLMF/ODS_Tools/pull/23) - Release/3.0.5
* [#24](https://github.com/OasisLMF/ODS_Tools/pull/24) - CI Fix - local spec install
* [#26](https://github.com/OasisLMF/ODS_Tools/pull/25) - Log message when a column is not part of OED schema
* [#28](https://github.com/OasisLMF/ODS_Tools/pull/29) - Strip out spaces in OED input headers
.. _`3.0.6`:  https://github.com/OasisLMF/ODS_Tools/compare/3.0.5...3.0.6

`3.0.5`_
 ---------
* [#5](https://github.com/OasisLMF/ODS_Tools/pull/1) - Separate the ods-tools code from the data standard
* [#8](https://github.com/OasisLMF/ODS_Tools/pull/9) - Parquet OED ReinsScope incorrectly fail validation
* [#12](https://github.com/OasisLMF/ODS_Tools/pull/13) - Analysis Settings in the  OasisPlatform OpenAPI schema fails build check
* [#15](https://github.com/OasisLMF/ODS_Tools/pull/15) - Release/3.0.4
* [#17](https://github.com/OasisLMF/ODS_Tools/pull/18) - Loading blank fields from DataFrame creates string value 'None'
* [#19](https://github.com/OasisLMF/ODS_Tools/pull/19) - support stream as Oed Source
* [#20](https://github.com/OasisLMF/ODS_Tools/pull/20) - detect stream_obj type if not set
* [#21](https://github.com/OasisLMF/ODS_Tools/pull/22) - LocPeril raise validation error when Blank but it should be allowed according to schema
.. _`3.0.5`:  https://github.com/OasisLMF/ODS_Tools/compare/3.0.4...3.0.5

`3.0.4`_
 ---------
* [#5](https://github.com/OasisLMF/ODS_Tools/pull/1) - Separate the ods-tools code from the data standard
* [#12](https://github.com/OasisLMF/ODS_Tools/pull/13) - Analysis Settings in the  OasisPlatform OpenAPI schema fails build check
* [#8](https://github.com/OasisLMF/ODS_Tools/pull/9) - Parquet OED ReinsScope incorrectly fail validation
.. _`3.0.4`:  https://github.com/OasisLMF/ODS_Tools/compare/3.0.3...3.0.4

