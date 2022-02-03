create or replace package body "RJ_PKG" is

function get_pipe( --http://marcsewtz.blogspot.com/2008/04/generating-csv-files-and-storing-them.html
    p_query VARCHAR2 )
  RETURN BLOB
IS
  l_cursor        INTEGER;
  l_cursor_status INTEGER;
  l_col_count     NUMBER;
  l_desc_tbl sys.dbms_sql.desc_tab2;
  l_col_val VARCHAR2(32767);
  l_row_num NUMBER;
  l_report BLOB;
  l_raw raw(32767);
BEGIN
  l_row_num := 1;
  -- open BLOB to store CSV file
  dbms_lob.createtemporary( l_report, FALSE );
  dbms_lob.open( l_report, dbms_lob.lob_readwrite );
  -- parse query
  l_cursor := dbms_sql.open_cursor;
  dbms_sql.parse(l_cursor, p_query, dbms_sql.native);
  dbms_sql.describe_columns2(l_cursor, l_col_count, l_desc_tbl );
  -- define report columns
  FOR i IN 1 .. l_col_count
  LOOP
    dbms_sql.define_column(l_cursor, i, l_col_val, 32767 );
  END LOOP;
l_cursor_status := sys.dbms_sql.execute(l_cursor);
-- write result set to CSV file
LOOP
  EXIT
WHEN dbms_sql.fetch_rows(l_cursor) <= 0;
  FOR i IN 1 .. l_col_count
  LOOP
    dbms_sql.column_value(l_cursor, i, l_col_val);
    IF i = l_col_count THEN
      --enclose each value in double quotes Chr(34):
      l_col_val := l_col_val||chr(10); --end of line, insert line break
    ELSE
      l_col_val := l_col_val||'|'; --insert pipe and keep going
    END IF;
    l_raw := utl_raw.cast_to_raw( l_col_val );
    dbms_lob.writeappend( l_report, utl_raw.length( l_raw ), l_raw );
  END LOOP;
  l_row_num := l_row_num + 1;
END LOOP;
dbms_sql.close_cursor(l_cursor);
dbms_lob.close( l_report );
-- return pipe delimited file
RETURN l_report;
END get_pipe;


function parse_importandload_response(p_clob IN clob)
   return number
   is 
    l_result     number;
    l_xml        xmltype;

begin
l_xml := XMLTYPE.createXML(p_clob);

SELECT docid into l_result
FROM
       XMLTable(  
             XMLNamespaces(  
               'http://schemas.xmlsoap.org/soap/envelope/' AS "SOAP-ENV"  
                ,'http://xmlns.oracle.com/apps/hcm/common/dataLoader/core/dataLoaderIntegrationService/types/' AS  "ns0"             
              ), 'SOAP-ENV:Envelope/SOAP-ENV:Body/ns0:importAndLoadDataResponse/ns0:result' 
              passing   l_xml
              columns docid number path '.'  
           ) ;
           
           RETURN l_result;

end parse_importandload_response;

function parse_ucm_response (p_clob IN clob)
   return varchar2
      
   is
   
l_start number;
l_end number;
l_clob clob;
l_xml xmltype;
l_data varchar2(60);


begin
--get rid of stuff before and after the envelope that makes this response to not be valid xml:
select instr(p_clob, '<?xml version="1.0"') into l_start from DUAL;
select instr(p_clob, '</env:Envelope>') into l_end from DUAL;
select substr(p_clob, l_start, (l_end - l_start) +15 ) into l_clob from DUAL;

--make the cleaned up clob an xmltype so we can parse as usual
l_xml := XMLTYPE.createXML(l_clob);

SELECT data  into l_data
       FROM 
       XMLTable(  
             XMLNamespaces(  
               'http://schemas.xmlsoap.org/soap/envelope/' AS "SOAP-ENV"  
                ,'http://xmlns.oracle.com/apps/financials/commonModules/shared/model/erpIntegrationService/types/' AS  "ns0" 
                , 'http://xmlns.oracle.com/apps/financials/commonModules/shared/model/erpIntegrationService/types/' AS "ns2"
                
              ), 'SOAP-ENV:Envelope/SOAP-ENV:Body/ns0:uploadFileToUcmResponse/ns2:result' 
              passing   l_xml 
              columns data clob path '.'  
           ) ;
           
           return l_data;

   end parse_ucm_response;

function element_entry_header
    return blob
    
    is
        l_blob blob;
 
    begin

    /*
    METADATA|ElementEntry|SourceSystemOwner|SourceSystemId|AssignmentNumber|EffectiveStartDate|EffectiveEndDate|ElementName|LegislativeDataGroupName|MultipleEntryCount|EntryType|CreatorType
    */
    l_blob := get_pipe(
        'SELECT 
        ''METADATA'', 
        ''ElementEntry'', 
        ''SourceSystemOwner'', 
        ''SourceSystemId'', 
        ''AssignmentNumber'', 
        ''EffectiveStartDate'', 
        ''EffectiveEndDate'',
        ''ElementName'',
        ''LegislativeDataGroupName'',
        ''MultipleEntryCount'',
        ''EntryType'',
        ''CreatorType''
        from DUAL
        '
    );
        
        return l_blob;
    
    
    end;
    
    function element_entry_value_header
    return blob
    
    is
        l_blob blob;
    
    begin
    /*
    METADATA|ElementEntryValue|SourceSystemOwner|SourceSystemId|ElementEntryId(SourceSystemId)|EffectiveStartDate|EffectiveEndDate|ElementName|LegislativeDataGroupName|MultipleEntryCount|AssignmentNumber|InputValueName|ScreenEntryValue
    */

    l_blob := get_pipe(
        'SELECT 
        ''METADATA'', 
        ''ElementEntryValue'', 
        ''SourceSystemOwner'', 
        ''SourceSystemId'', 
        ''ElementEntryId(SourceSystemId)'', 
        ''EffectiveStartDate'', 
        ''EffectiveEndDate'',
        ''ElementName'',
        ''LegislativeDataGroupName'',
        ''MultipleEntryCount'',
        ''AssignmentNumber'',
        ''InputValueName'',
        ''ScreenEntryValue''
        from DUAL
        '
    );
        
        return l_blob;
    
    end;
    
    function build_ucm_payload_eseu(p_person_number IN number, p_amount IN number)
    return clob
    
    is
    
    l_blob                          blob;
    l_base64                        clob;
    l_zip                           blob;
    l_element_entry_header          blob;
    l_element_entry_value_header    blob;
    l_element_entry                 blob;
    l_element_entry_values          blob;
    l_person_number                 number;
    
    begin
    
    l_element_entry_header := element_entry_header;
    l_element_entry_value_header := element_entry_value_header;


    
     --TODO: Add WHERE clause that will get passed as an input param
     /*
     MERGE|ElementEntry|STOCK|541_ELEM_ENTR1|E541|2021/11/18|4712/12/31|Stock Purchase|US Legislative Data Group|1|E|H
     */
    l_element_entry := get_pipe(
        
        'SELECT 
        ''MERGE'', 
        ''ElementEntry'',
        ''STOCK'',
        '''||p_person_number|| '_ELEM_ENTR1'||''',
        '''||'E'|| p_person_number||''',
        ''2021/11/23'',
        ''4712/12/31'',
        ''Stock Purchase'',
        ''US Legislative Data Group'',
         ''1'',
        ''E'',
        ''H''
        from dual
        '
    );
    
    --TODO: Add WHERE clause that will get passed as an input param
    /*
    MERGE|ElementEntryValue|STOCK|USDG_COMM_E541|541_ELEM_ENTR1|2021/11/18|4712/12/31|Stock Purchase|US Legislative Data Group|1|E541|Amount|1000
    */
    
    l_element_entry_values := get_pipe(
    'SELECT
        ''MERGE'',
        ''ElementEntryValue'',
        ''STOCK'',
         '''||'USDG_COMM_E'|| p_person_number||''',
         '''||p_person_number|| '_ELEM_ENTR1'||''',
        ''2021/11/23'',
        ''4712/12/31'',
        ''Stock Purchase'',
        ''US Legislative Data Group'',
        ''1'',
        '''||'E'|| p_person_number||''',
        ''Amount'',
        '''||p_amount||'''
        from
        dual
        ');
           
        dbms_lob.createtemporary( l_blob, FALSE );
        dbms_lob.open( l_blob, dbms_lob.lob_readwrite );
        
        dbms_lob.append(l_blob, l_element_entry_header);
        dbms_lob.append(l_blob, l_element_entry);
        dbms_lob.append(l_blob, l_element_entry_value_header);
        dbms_lob.append(l_blob, l_element_entry_values);
        
        apex_zip.add_file (
            p_zipped_blob => l_zip,
            p_file_name   => 'ElementEntry.dat',
            p_content     => l_blob 
        ); 
        
        dbms_lob.close(l_blob);
        
        apex_zip.finish(p_zipped_blob => l_zip);
    
        --convert zip to base64 for use in web service SOAP envelope:      
        l_base64 := apex_web_service.blob2clobbase64(l_zip);
        dbms_lob.freetemporary( l_blob );

    return l_base64;
    
    end build_ucm_payload_eseu;
    
    function hcm_import(p_content_id IN varchar2)
    return number
    
    is
    l_envelope     clob;
    l_ws_response  clob;
    l_response     number;
    
    
    begin


    l_envelope := '
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:typ="http://xmlns.oracle.com/apps/hcm/common/dataLoader/core/dataLoaderIntegrationService/types/">
       <soapenv:Header/>
       <soapenv:Body>
          <typ:importAndLoadData>
             <typ:ContentId>'||p_content_id||'</typ:ContentId>
             <typ:Parameters></typ:Parameters>
          </typ:importAndLoadData>
       </soapenv:Body>
    </soapenv:Envelope>';
    
      apex_web_service.g_request_headers(1).name  := 'SOAPAction'; 
      apex_web_service.g_request_headers(1).value := 'http://xmlns.oracle.com/apps/hcm/common/dataLoader/core/dataLoaderIntegrationService/types/'; 
      apex_web_service.g_request_headers(2).name  := 'Content-Type';
      apex_web_service.g_request_headers(2).value := 'text/xml; charset=UTF-8'; 

      l_ws_response := apex_web_service.make_rest_request(
        p_url          => 'https://'||g_instance||'-saasfademo1.ds-fa.oraclepdemos.com:443/hcmService/HCMDataLoader',
        p_http_method  => 'POST',
        p_body         => l_envelope,
        p_username     => 'betty.anderson',
        p_password     => g_password);

        l_response := parse_importandload_response(l_ws_response);
        
        return l_response;
    
    end hcm_import;
    
procedure load_to_ucm(p_record_id IN number)
    is
    l_ws_response        clob;
    l_ucm_content        clob;
    l_envelope           clob;
    l_id                 number;
    l_ucm_id             number;
    l_ess_id             number;
    l_person_number      number;
    l_amount             number;

    begin  

    select person_number, shares into l_person_number, l_amount
    from rj_stock_purchases p
    left join hcm_employees e on p.person_id = e.person_id  where id = p_record_id;

    l_ucm_content := build_ucm_payload_eseu(p_person_number => l_person_number, p_amount => l_amount);
    
    l_id := generic_seq.nextval;
    
    l_envelope := '
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:typ="http://xmlns.oracle.com/apps/financials/commonModules/shared/model/erpIntegrationService/types/" xmlns:erp="http://xmlns.oracle.com/apps/financials/commonModules/shared/model/erpIntegrationService/">
   <soapenv:Header/>
   <soapenv:Body>
      <typ:uploadFileToUcm>
         <typ:document>
            <erp:Content>'||l_ucm_content||'</erp:Content>
            <erp:FileName>ElementEntry'||l_id||'.zip</erp:FileName>
            <erp:ContentType>zip</erp:ContentType>
            <erp:DocumentTitle>ElementEntry'||l_id||'.zip</erp:DocumentTitle>
            <erp:DocumentAuthor>curtis.feitty</erp:DocumentAuthor>
            <erp:DocumentSecurityGroup>FAFusionImportExport</erp:DocumentSecurityGroup>
            <erp:DocumentAccount>hcm/dataloader/import</erp:DocumentAccount>
            <erp:DocumentName>ElementEntry'||l_id||'</erp:DocumentName> 
         </typ:document>
      </typ:uploadFileToUcm>
   </soapenv:Body>
</soapenv:Envelope>
    ';
    
  apex_web_service.g_request_headers(1).name  := 'SOAPAction'; 
  apex_web_service.g_request_headers(1).value := 'http://xmlns.oracle.com/apps/financials/commonModules/shared/model/erpIntegrationService/uploadFileToUcm'; 
  apex_web_service.g_request_headers(2).name  := 'Content-Type';
  apex_web_service.g_request_headers(2).value := 'text/xml; charset=UTF-8'; 

--make SOAP call:
    l_ws_response := apex_web_service.make_rest_request(
    p_url          => 'https://'||g_instance||'-saasfademo1.ds-fa.oraclepdemos.com:443/fscmService/ErpIntegrationService',
    p_http_method  => 'POST',
    p_body         => l_envelope,
    p_username     => 'betty.anderson',
    p_password     => g_password);
  
       --l_ucm_id := parse_ucm_response(l_ws_response);
       --update rj_stock_purhcases set ucm_id = l_ucm_id;
       
       --run hcm import process passing the UCM content id (soap api):
       l_ess_id := hcm_import('ELEMENTENTRY'||l_id);
       
       update rj_stock_purchases set ess_id = l_ess_id, api_status_code = apex_web_service.g_status_code where id = p_record_id;
     
       if apex_web_service.g_status_code = 200
       then
       update rj_stock_purchases set status = 'Processed' where id = p_record_id;
       end if;

   
 
    end;
    
end "RJ_PKG";
