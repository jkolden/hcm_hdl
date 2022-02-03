create or replace package rj_pkg as
g_instance         varchar2(30) default 'fa-eqkg-dev3';
g_password         varchar2(15) default 'jBf8Y%7%';
g_session_id       varchar2(120);
    
procedure load_to_ucm(p_record_id IN number);

end;
