############################################
####	Base Difusión Proceso 2021	#####
############################################
drop table if exists AlbertoC.ba_alumnos_carrera_difusion;
create table AlbertoC.ba_alumnos_carrera_difusion as
SELECT safe_cast(RUT as int64) as rut, 
       null as dif_codigo_carrera, 
       safe_cast(RBD as int64) as dif_rbd, 
       count(*) as dif_toques,  
       max(fecha) as dif_ult_fecha,
       EMAIL as dif_mail,
       safe_cast(CELULAR as int64) as dif_celular,
       safe_cast(FONO_FIJO as int64) as dif_fono_fijo,
       REGION as dif_region,
       NOMBRES as nombre,
       AP_PATERNO as apellido_paterno,
       AP_MATERNO as apellido_materno
FROM `usansebastian.p2021.ba_difusion` 
### where format_date('%E4Y',fecha) in ('2019') 
GROUP BY safe_cast(RUT as int64), dif_codigo_carrera, 
       safe_cast(RBD as int64), dif_mail, safe_cast(CELULAR as int64),
       safe_cast(FONO_FIJO as int64), dif_region, nombre, apellido_paterno, apellido_materno;
	   
	   

drop table if exists AlbertoC.ba_alumnos_carrera_difusion_v2;
create table AlbertoC.ba_alumnos_carrera_difusion_v2 as
select rut, 
       nombre,
       apellido_paterno,
       apellido_materno,
       dif_region,
       dif_codigo_carrera, 
       dif_rbd, 
       dif_toques, 
       dif_ult_fecha, 
       dif_mail,
       dif_celular,
       dif_fono_fijo,
row_number() over (partition by rut order by dif_toques desc, dif_ult_fecha desc, dif_region desc, dif_mail desc,
dif_celular desc, dif_fono_fijo desc, nombre desc, apellido_paterno desc, apellido_materno desc) as ranking
from AlbertoC.ba_alumnos_carrera_difusion
where rut is not null;


drop table if exists AlbertoC.ba_alumnos_difusion;
create table AlbertoC.ba_alumnos_difusion as
select safe_cast(rut as int64) as rut, 
       count(*) as dif_toques_total
from usansebastian.p2021.ba_difusion
###  where format_date('%E4Y',fecha) in ('2019')
group by rut;


drop table if exists AlbertoC.rbd;
create table AlbertoC.rbd as
SELECT RBD as rbd, count(RBD) as seleccionados, 
       round(avg(PROMLM),0) as prom_lym,       
       case when round(avg(ING_BRUTO_FAMILIAR),0) between 1 and 6 then 1 
            else 0 
        end as decil_1_a_6     
FROM usansebastian.p2021.ba_postulacion
WHERE ESTADO_DE_LA_POSTULACION = 24 and rbd is not null
GROUP BY rbd ;



drop table if exists AlbertoC.ba_alumnos_difusion_v2;
create table AlbertoC.ba_alumnos_difusion_v2 as
select a.rut, 
       nombre,
       apellido_paterno,
       apellido_materno,
       dif_region,
       1 as dif_tocado, 
       a.dif_codigo_carrera, 
       d.codigo_demre as dif_codigo_carrera_demre, 
       a.dif_rbd, 
       a.dif_mail,
       a.dif_celular,
       a.dif_fono_fijo,
       a.dif_toques as dif_toques_carrera, 
       b.dif_toques_total, 
       coalesce(c.prom_lym,525) as dif_prom_lym, 
       coalesce(c.decil_1_a_6,1) as dif_decil_1_a_6
from AlbertoC.ba_alumnos_carrera_difusion_v2 a
left join AlbertoC.ba_alumnos_difusion b on a.rut=b.rut
left join AlbertoC.rbd c on a.dif_rbd=c.rbd
left join p2021.ba_programa d on a.dif_codigo_carrera=d.codigo_demre
where a.ranking=1;


############################################
####	Base simulación Proceso 2021	####
############################################
drop table if exists AlbertoC.ba_alumnos_carrera_simulacion;
create table AlbertoC.ba_alumnos_carrera_simulacion as
select rut, 
       CODIGO_CARRERA as sim_codigo_carrera, 
       count(*) as sim_cantidad, 
       max(FECHA) as sim_ult_fecha,
       EMAIL as sim_mail,
       safe_cast(CELULAR as int64) as sim_celular,
       safe_cast(TELEFONO_FIJO as int64) as sim_fono_fijo,
       (safe_cast(PUNTAJE_LENGUAJE as int64)+ safe_cast(PUNTAJE_MATEMATICA as int64))/2 as sim_prom_lm,
       UTILIZA_CAE  as sim_cae,
       UTILIZA_BECA_ESTATAL as sim_beca_bicentenario  ,
       NOMBRES as nombre,
       PATERNO as apellido_paterno,
       MATERNO as apellido_materno  
from p2021.ba_simulacion
group by rut, sim_codigo_carrera, email, safe_cast(celular as int64),safe_cast(telefono_fijo as int64), sim_prom_lm, 
             utiliza_cae, utiliza_beca_estatal, nombre, apellido_paterno, apellido_materno;

			 

drop table if exists AlbertoC.ba_alumnos_carrera_simulacion_v2;
create table AlbertoC.ba_alumnos_carrera_simulacion_v2 as
select rut, 
       nombre,
       apellido_paterno,
       apellido_materno,
       sim_codigo_carrera, 
       sim_cantidad, 
       sim_ult_fecha, 
       sim_prom_lm,
       sim_cae,
       sim_beca_bicentenario,
       sim_mail,
       sim_celular,
       sim_fono_fijo,
       row_number() over (partition by rut order by sim_cantidad desc, sim_ult_fecha desc, sim_prom_lm desc, sim_cae desc, 
                          sim_beca_bicentenario desc, sim_mail desc, sim_celular desc, sim_fono_fijo desc, nombre desc, 
                          apellido_paterno desc, apellido_materno desc) as ranking
from AlbertoC.ba_alumnos_carrera_simulacion;



###	CANTIDAD DE SIMULACIONES POR RUT
drop table if exists AlbertoC.ba_alumnos_simulacion;
create table AlbertoC.ba_alumnos_simulacion as
select rut, count(*) as sim_cantidad_total
from p2021.ba_simulacion
group by rut;



drop table if exists AlbertoC.ba_alumnos_simulacion_v2;
create table AlbertoC.ba_alumnos_simulacion_v2 as
select a.rut, 
       nombre,
       apellido_paterno,
       apellido_materno,
       1 as simula, 
       a.sim_codigo_carrera, 
       --d.codigo_demre as sim_codigo_carrera_demre,
       a.sim_cantidad as sim_cantidad_carrera, 
       b.sim_cantidad_total,
       a.sim_prom_lm,
       a.sim_cae,
       a.sim_beca_bicentenario,
       a.sim_mail,
       a.sim_celular,
       a.sim_fono_fijo,
from AlbertoC.ba_alumnos_carrera_simulacion_v2 a
left join AlbertoC.ba_alumnos_simulacion b on a.rut=b.rut
--left join p2021.ba_programa d on a.sim_codigo_carrera=d.codigo_carrera
where a.ranking=1;


###########################################################
###	Base Convocado y Lista de Espera proceso 2021	###
###########################################################

###	Creación tabla de Ranking nueva para USS
drop table if exists AlbertoC.ba_postulacion_ranking;
create table AlbertoC.ba_postulacion_ranking as
SELECT rut, rbd, nombres, paterno, materno, codigo,
       email,NUMERO_DE_CELULAR,NRO_DE_TELEFONO, PUNTAJE_PONDERADO, ptje_nem,ptje_ranking, 
       LENG_Y_COM , 
       MATEMATICA, 
       HIST_CS_SOC, 
       CIENCIAS, 
       PROMLM, 
       ing_bruto_familiar,
       preferencia, ESTADO_DE_LA_POSTULACION,
       row_number() OVER (partition by rut order by ESTADO_DE_LA_POSTULACION asc, PREFERENCIA asc) AS ranking,
       row_number() OVER (partition by rut, ESTADO_DE_LA_POSTULACION order by PREFERENCIA asc) AS ranking_2
FROM `usansebastian.p2021.ba_postulacion`


drop table if exists AlbertoC.ba_alumnos_postulacion;
create table AlbertoC.ba_alumnos_postulacion as
select rut, 1 as post_seleccionado,
       rbd as post_rbd, 
       nombres,
       paterno,
       materno,
       email as post_mail,
       NUMERO_DE_CELULAR as post_celular,
       NRO_DE_TELEFONO as post_fono_fijo,
       ptje_nem as post_ptje_nem, 
       ptje_ranking as post_ptje_ranking, 
       LENG_Y_COM as post_ptje_leng, 
       MATEMATICA as post_ptje_mat, 
       HIST_CS_SOC as post_ptje_hysc, 
       CIENCIAS as post_ptje_ciencia, 
       PROMLM as post_prom_lym, 
       ing_bruto_familiar as post_decil,
       preferencia as post_preferencia,
        case 
            when ing_bruto_familiar is null then null 
            when coalesce(ing_bruto_familiar,0) between 1 and 7 then 1 
            else 0 
        end as post_decil_1_a_7,
        case 
            when ing_bruto_familiar is null then null 
            when coalesce(ing_bruto_familiar,0) between 1 and 6 then 1 
            else 0
         end as post_decil_1_a_6,
from AlbertoC.ba_postulacion_ranking
where ranking=1 and ranking_2 = 1;


###################################
###	Base Convocados 2021	###
###################################
drop table if exists AlbertoC.ba_alumnos_convocados;
create table AlbertoC.ba_alumnos_convocados as
select RUT, 
       1 as conv_convocado, 
       CODIGO as conv_codigo_carrera_demre, 
       PUNTAJE_PONDERADO as conv_puntaje_ponderado
from AlbertoC.ba_postulacion_ranking
where ESTADO_DE_LA_POSTULACION=24 and ranking_2=1;


###########################################
###	Base Lista de Espera 2021	###
###########################################
drop table if exists AlbertoC.ba_alumnos_lista_espera;
create table AlbertoC.ba_alumnos_lista_espera as
select rut, 
       1 as lespera_enlistaespera, 
       CODIGO as lespera_codigo_carrera_demre, 
       puntaje_ponderado as leespera_puntaje_podenrado
from AlbertoC.ba_postulacion_ranking
where estado_de_la_postulacion=25 and ranking_2=1;



###########################################
###	Base Matrícula proceso 2021	###
###########################################
drop table if exists AlbertoC.ba_alumnos_matricula;
create table AlbertoC.ba_alumnos_matricula as
select * from( select
rut,            
codigo_carrera as matricula_codigo_carrera, 
           codigo_demre as matricula_codigo_demre, 
                tipo_matricula as matricula_tipo, 
                estado_preferencia_sua as matricula_estado,
                1 as matricula_antiguedad,
                calidad_ingreso as matricula_calidad_ingreso, 
                tipo_programa as matricula_tipo_programa, 
                fecha_matricula as matricula_fecha,
                row_number() over (partition by rut order by fecha_matricula desc) as ranking,
                ano_egreso_colegio as egreso ,  
                comuna as comuna,
                rbd_colegio as rbd_matricula , 
                dependencia as dependencia_colegio,
				fecha_nacimiento as fecha_nacimiento

from p2021.ba_matricula)
where ranking=1;


###################################################
###	Base Alumnos Beca Interna proceso 2021	###
###################################################
drop table if exists AlbertoC.ba_alumnos_becas_interna;
create table AlbertoC.ba_alumnos_becas_interna as 
select rut,
                            sum(coalesce(monto_beca_matricula,0)) as monto_beca_matricula, 
                            sum(coalesce(monto_beca_arancel,0)) as monto_beca_arancel, 
                            sum(coalesce(MONTO_DESCUENTO_ARANCEL,0)) as monto_beca_arancel_extra, 
                            case 
                                    when coalesce(monto_beca_matricula,0)>0 then 1 
                                    else 0 
                            end as beca_matricula, 
                            case 
                                    when coalesce(monto_beca_arancel,0)>0 then 1 
                                    else 0 
                            end as beca_arancel,
                            case 
                                    when coalesce(MONTO_DESCUENTO_ARANCEL,0)>0 then 1 
                                    else 0 
                            end as beca_arancel_extra
from p2021.ba_beneficios_internos
group by rut, beca_matricula, beca_arancel, beca_arancel_extra


###	CASO PARTICULAR	(solo para validar información)
SELECT *
FROM `usansebastian.p2020.ba_beneficios_internos`
where rut = 26729054
LIMIT 1000


###########################################
###	Base Alumnos CAE proceso 2021	###
###########################################
drop table if exists AlbertoC.ba_alumnos_cae;
create table AlbertoC.ba_alumnos_cae as 
select rut, 
       1 as cae
from p2020.ba_cae;


###################################################
###	Base Alumnos Beca Externa Proceso 2021	###
###################################################
drop table if exists AlbertoC.ba_alumnos_becas_externas;
create table AlbertoC.ba_alumnos_becas_externas as 
SELECT rut, case when glosa_bea=1 or glosa_bjgm=1 or glosa_bhpe= 1 then 1 
            else 0 end beca_externa,
       GLOSA_BVP as bvp, 
       GLOSA_BB as bb, 
       GLOSA_BPSU bpsu, 
       GLOSA_BEA as bea, 
       GLOSA_BJGM as bjgm, 
       GLOSA_BNM as bnm, 
       GLOSA_BHPE as bhpe,  
       GLOSA_FSCU as fscu
FROM `usansebastian.p2021.ba_beneficios_externos`;


###########################################################
###	      Agrupación datos becas interna             ###
###########################################################
drop table if exists AlbertoC.ba_alumnos_becas_interna_v2;
create table AlbertoC.ba_alumnos_becas_interna_v2 as
SELECT rut, SUM(monto_beca_matricula) as monto_beca_matricula, SUM(monto_beca_arancel) as monto_beca_arancel,
SUM(monto_beca_arancel_extra) as monto_beca_arancel_extra, SUM(beca_matricula) as beca_matricula,
SUM(beca_arancel) as beca_arancel, SUM(beca_arancel_extra) as beca_arancel_extra 
FROM AlbertoC.ba_alumnos_becas_interna  
GROUP BY rut;


###########################################################
###	         Eliminar duplicados cae                 ###
###########################################################
drop table if exists AlbertoC.ba_alumnos_cae_v2;
create table AlbertoC.ba_alumnos_cae_v2 as
SELECT rut,cae FROM
(SELECT *, ROW_NUMBER() OVER(Partition BY rut order By rut) as RowNumber from AlbertoC.ba_alumnos_cae)
WHERE RowNumber = 1


###########################################################
###	         Ruts Unicos todas las tabla             ###
###########################################################

drop table if exists AlbertoC.ba_alumnos_rut_2021;
create table AlbertoC.ba_alumnos_rut_2021 as
select distinct rut from AlbertoC.ba_alumnos_difusion_v2
union distinct
select distinct rut from AlbertoC.ba_alumnos_simulacion_v2
union distinct
select distinct rut from AlbertoC.ba_alumnos_postulacion
union distinct 
select distinct rut from AlbertoC.ba_alumnos_convocados
union distinct 
select distinct rut from AlbertoC.ba_alumnos_lista_espera 
union distinct 
select distinct rut from AlbertoC.ba_alumnos_matricula 
union distinct 
select distinct rut from AlbertoC.ba_alumnos_becas_interna 
union distinct 
select distinct rut from AlbertoC.ba_alumnos_cae
union distinct 
select distinct rut from AlbertoC.ba_alumnos_becas_externas;



###########################################################
###	       Creación Base Analítica Proceso 2021      ###
###########################################################
drop table if exists AlbertoC.ba_alumnos_p2021;
create table AlbertoC.ba_alumnos_p2021 as
select a.rut, 
       --m.dv,
	   null as dv,
       coalesce(b.dif_tocado,0) as dif_tocado, 
       #coalesce(b1.dif_tocado,0) as dif_tocado_2018, 
       #coalesce(b2.dif_tocado,0) as dif_tocado_2019, 
        b.dif_codigo_carrera,
        b.dif_codigo_carrera_demre, 
        b.dif_rbd, 
        b.dif_toques_total,
       b.dif_prom_lym, 
       case 
            when b.dif_prom_lym is null then null 
            when b.dif_prom_lym <=450 then 0 
            when b.dif_prom_lym between 451 and 500 then 450 
            when b.dif_prom_lym between 501 and 520 then 500 
            when b.dif_prom_lym between 521 and 530 then 520 
            when b.dif_prom_lym between 531 and 551 then 530 
            when b.dif_prom_lym between 552 and 581 then 551 
            when b.dif_prom_lym between 582 and 601 then 581 
            when b.dif_prom_lym between 602 and 620 then 601 
            else 620
        end as dif_rango_psu, 
        b.dif_region,
        --#case when upper(b.dif_region) like '%SERENA%' or  upper(b.dif_region) like '%COQUI%' or  upper(b.dif_region) like '%COPIA%' or  upper(b.dif_region) like '%VALLENA%' or upper(b.dif_region) like '%OVALLE%' or  upper(b.dif_region) like '%VICU%' then 'SERENA' else 'SANTIAGO' end as dif_sede,       
        b.dif_decil_1_a_6, 
        coalesce(c.simula,0) as simula, 
        c.sim_codigo_carrera, 
        c.sim_cantidad_carrera, 
        --#c.sim_codigo_carrera_demre,
        c.sim_cantidad_total, 
        c.sim_prom_lm,
        c.sim_beca_bicentenario,
        c.sim_cae,
        --#p.PTJE_NEM as psu_nem_2019,
        --#p.PTJE_RANKING as psu_ranking_2019,	
        --#p.LENG_ACTUAL as psu_leng_2019,	
        --#p.MATE_ACTUAL as psu_mate_2019,
        --#p.HCSO_ACTUAL as psu_hcso_2019, 	
        --#p.CIEN_ACTUAL as psu_cien_2019,
        --#(p.LENG_ACTUAL+p.MATE_ACTUAL)/2 as psu_prom_lym_2019,
        --#p.BEA as psu_bea_2019,
        --#s.CODIGO_REGION region_2019,
        s.PTJE_NEM as psu_nem_2020,
        s.PTJE_RANKING as psu_ranking_2020,	
        s.LENG_ACTUAL as psu_leng_2020,	
        s.MATE_ACTUAL as psu_mate_2020,
        s.HCSO_ACTUAL as psu_hcso_2020, 	
        s.CIEN_ACTUAL as psu_cien_2020,
        (s.LENG_ACTUAL+s.MATE_ACTUAL)/2 as psu_prom_lym_2020,
        s.BEA as psu_bea_2020,
        s.CODIGO_REGION region_2020,
        --m.PTJE_NEM as psu_nem_2021,
        --m.PTJE_RANKING as psu_ranking_2021,	
        --m.LENG_ACTUAL as psu_leng_2021,	
        --m.MATE_ACTUAL as psu_mate_2021,
        --m.HCSO_ACTUAL as psu_hcso_2021, 	
        --m.CIEN_ACTUAL as psu_cien_2021,
        --(m.LENG_ACTUAL+m.MATE_ACTUAL)/2 as psu_prom_lym_2021,
        --m.BEA as psu_bea_2021,
        --m.CODIGO_REGION as region_2021,
        --coalesce(m.RAMA_EDUCACIONAL) as tipo_colegio,
        --coalesce(m.ANO_EGRESO) as egreso,
        --coalesce(m.CODIGO_COMUNA) as cod_comuna,
		null as psu_nem_2021,
        null as psu_ranking_2021,	
        null as psu_leng_2021,	
        null as psu_mate_2021,
        null as psu_hcso_2021, 	
        null as psu_cien_2021,
        null as psu_prom_lym_2021,
        null as psu_bea_2021,
        null as region_2021,
        null as tipo_colegio,
        null as egreso,
        null as cod_comuna,
        --case
        --      when (m.MATE_ACTUAL+m.LENG_ACTUAL)/2>=450  then 1
        --      when (m.MATE_ACTUAL+m.LENG_ACTUAL)/2>=450  or (s.LENG_ACTUAL+s.MATE_ACTUAL)/2>=450 then 1
        --      else 0
        --end as psu_prom_lym_valido,
		null as psu_prom_lym_valido,
        --case
        --when (m.MATE_ACTUAL+m.LENG_ACTUAL)/2  IS NOT NULL and (s.LENG_ACTUAL+s.MATE_ACTUAL)/2 IS NOT NULL then       
        --       case
        --       when (m.MATE_ACTUAL+m.LENG_ACTUAL)/2> (s.LENG_ACTUAL+s.MATE_ACTUAL)/2 then ROUND ((m.MATE_ACTUAL+m.LENG_ACTUAL)/2,0)
        --       else  ROUND ((s.LENG_ACTUAL+s.MATE_ACTUAL)/2,0)
        --       end
        --else coalesce (  ROUND ((m.MATE_ACTUAL+m.LENG_ACTUAL)/2,0) ,  ROUND ((s.LENG_ACTUAL+s.MATE_ACTUAL)/2,0),0)
        --end as psu_prom_lym_final,
		null as psu_prom_lym_final,
        --coalesce(m.grupo_depencia,0) as dependencia,
		null as dependencia,
        --coalesce(m.rbd) as rbd,
       #safe_cast(o.GLOSA_BB as int64) as glosa_bb,
       coalesce(d.post_seleccionado,0) as post_seleccionado,
       d.post_rbd,
       d.post_preferencia, 
       d.post_ptje_nem, 
       d.post_ptje_ranking, 
       d.post_ptje_leng, 
       d.post_ptje_mat, 
       d.post_ptje_hysc, 
       d.post_ptje_ciencia, 
       d.post_prom_lym, 
       d.post_decil, 
       d.post_decil_1_a_7, 
       d.post_decil_1_a_6, 
       case 
           when d.post_prom_lym is null then null
           when d.post_prom_lym <=450 then '<=450'
           when d.post_prom_lym between 451 and 500 then '451-500'
           when d.post_prom_lym between 501 and 520 then '501-520'
           when d.post_prom_lym between 521 and 530 then '521-530'
           when d.post_prom_lym between 531 and 551 then '531-551'
           when d.post_prom_lym between 552 and 581 then '552-581'
           when d.post_prom_lym between 582 and 601 then '582-601'
           when d.post_prom_lym between 602 and 620 then '602-620'
           else '>=621' 
       end as rango_psu,
       case 
           when d.post_prom_lym is null then null 
           when d.post_prom_lym <=450 then 0
           when d.post_prom_lym between 451 and 500 then 450
           when d.post_prom_lym between 501 and 520 then 500
           when d.post_prom_lym between 521 and 530 then 520
           when d.post_prom_lym between 531 and 551 then 530
           when d.post_prom_lym between 552 and 581 then 551
           when d.post_prom_lym between 582 and 601 then 581
           when d.post_prom_lym between 602 and 620 then 601 
           else 620 
       end as rango_psu_2, 
       case
            when d.post_prom_lym>=620 then 620
            when d.post_prom_lym>=600 then 600
            when d.post_prom_lym>=580 then 580
            when d.post_prom_lym>=550 then 550
            when d.post_prom_lym>=530 then 530
            when d.post_prom_lym>=520 then 520
            when d.post_prom_lym>=500 then 500
            else 0
        end as rango_beca_complementaria,
        case
            when d.post_prom_lym>=720 then 720
            when d.post_prom_lym>=700 then 700
            when d.post_prom_lym>=680 then 680
            when d.post_prom_lym>=660 then 660
            when d.post_prom_lym>=640 then 640
            when d.post_prom_lym>=620 then 620
            when d.post_prom_lym>=600 then 600
            else 0
        --end as rango_beca_ucen, **********
		  end as rango_beca_uss,
        --case
        --     when j.bjgm=1 and (coalesce((m.LENG_ACTUAL+m.MATE_ACTUAL)/2,0))>=500 then "BB"
        --    when j.bea=1 then "BEA"
        --    when j.bhpe=1 then "BHPE"
        --     when h.cae=1 and (coalesce((m.LENG_ACTUAL+m.MATE_ACTUAL)/2,0))>=475 then "CAE"
             --else "SB"
        --          when j.bjgm=1 and ( case when coalesce((m.LENG_ACTUAL+m.MATE_ACTUAL)/2,0)>=coalesce((s.LENG_ACTUAL+s.MATE_ACTUAL)/2,0) then coalesce((m.LENG_ACTUAL+m.MATE_ACTUAL)/2,0) else coalesce((s.LENG_ACTUAL+s.MATE_ACTUAL)/2,0) end)>=500 then "BB"
        --      when j.bea=1 then "BEA"
        --      when j.bhpe=1 then "BHPE"
         --     when h.cae=1 and ( case when coalesce((m.LENG_ACTUAL+m.MATE_ACTUAL)/2,0)>=coalesce((s.LENG_ACTUAL+s.MATE_ACTUAL)/2,0) then coalesce((m.LENG_ACTUAL+m.MATE_ACTUAL)/2,0) else coalesce((s.LENG_ACTUAL+s.MATE_ACTUAL)/2,0) end)>=475 then "CAE"
         --     else "SB"
        --end as tipo_alumno,
		null as tipo_alumno,
       coalesce(conv_convocado,0) as convocado, 
       conv_codigo_carrera_demre, 
       conv_puntaje_ponderado, 
       r.sede as conv_sede,
       --#r.facultad as conv_facultad,
       r.nombre_carrera as conv_carrera,
       coalesce(lespera_enlistaespera,0) as lista_espera, 
       lespera_codigo_carrera_demre, 
       leespera_puntaje_podenrado,
       lp.nombre_carrera as lespera_nombre_carrera,
       lp.sede as lespera_sede,
       matricula_codigo_carrera, 
       matricula_codigo_demre, 
       matricula_fecha,
       k.sede as matricula_sede, 
       --k.monto_arancel_referencia as monto_arancel_referencia,^^^^^^^^^^^^^^^^
	   null as monto_arancel_referencia,
       --#k.facultad as matricula_facultad,
       k.nombre_carrera as matricula_nombre_carrera,
        --k.nombre_carrera as matricula_nombre_carrera,
       matricula_tipo, 
       matricula_calidad_ingreso, 
       matricula_estado,
       matricula_tipo_programa, 
       matricula_antiguedad,
       --#matricula_demre, 
       --#matricula_vespertino, 
       --#matricula_tecnico, 
       --#matricula_demre_sua,
       coalesce(monto_beca_matricula,0) as monto_beca_matricula, 
       coalesce(monto_beca_arancel,0) as monto_beca_arancel, 
       coalesce(beca_matricula,0) as beca_matricula, 
       coalesce(beca_arancel,0) as beca_arancel, 
       coalesce(cae,0) as cae, 
       coalesce(beca_externa,0) as beca_externa, 
       coalesce(j.bb,0) as beca_bicentenario, 
       coalesce(j.bjgm,0) as beca_bjgm, 
       coalesce(j.bea,0) as bea,
       coalesce(j.bhpe,0) as bhpe,
       coalesce(j.bnm,0) as bnm,
       250000 as valor_matricula, 
       k.monto_arancel as valor_arancel, 
       --#k.arancel_ref as valor_arancel_referencia, 
       case 
           when coalesce(matricula_codigo_demre,0)>0 then matricula_codigo_demre
           when coalesce(conv_codigo_carrera_demre,0)>0 then conv_codigo_carrera_demre
           when coalesce(lespera_codigo_carrera_demre,0)>0 then lespera_codigo_carrera_demre 
           else b.dif_codigo_carrera_demre 
       end as codigo_carrera_demre_final, 
       case 
            when post_prom_lym is not null then post_prom_lym 
            else b.dif_prom_lym 
       end as post_lym_final, 
       d.post_decil_1_a_6 as decil_1_a_6_final,
       coalesce(--ROUND(v.PUNTAJE_PONDERADO,0) , ^^^^^^^^^^^^^^^^
	   ROUND(post_prom_lym,0),--ROUND((m.LENG_ACTUAL+m.MATE_ACTUAL)/2,0),
	   ROUND((s.LENG_ACTUAL+s.MATE_ACTUAL)/2,0) , 0) as PUNTAJE_PONDERADO,
g.egreso as egreso_matricula ,
g.comuna as comuna_matricula,
coalesce(g.rbd_matricula,b.dif_rbd) as rbd,
g.dependencia_colegio as dependencia_colegio,
g.fecha_nacimiento,
from AlbertoC.ba_alumnos_rut_2021 a
left join AlbertoC.ba_alumnos_difusion_v2 b on a.rut=b.rut
--#left join p2018.ba_alumnos_difusion_v2 b1 on a.rut=b1.rut
--#left join p2019.ba_alumnos_difusion_v2 b2 on a.rut=b2.rut
left join AlbertoC.ba_alumnos_simulacion_v2 c on a.rut=c.rut
left join AlbertoC.ba_alumnos_postulacion d on a.rut=d.rut
left join AlbertoC.ba_alumnos_convocados e on a.rut=e.rut
left join AlbertoC.ba_alumnos_lista_espera f on a.rut=f.rut
left join AlbertoC.ba_alumnos_matricula g on a.rut=g.rut
left join AlbertoC.ba_alumnos_cae_v2 h on a.rut=h.rut
left join AlbertoC.ba_alumnos_becas_interna_v2 i on a.rut=i.rut
left join AlbertoC.ba_alumnos_becas_externas j on a.rut=j.rut
left join p2021.ba_programa k on g.matricula_codigo_demre=k.codigo_demre
--#left join p2020.decil_alumno n on a.rut=n.rut *******
---left join p2021.ba_psu m on a.rut=m.NUMERO_DOCUMENTO ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
--#left join p2020.preselec_becas_20200213 o on a.rut=o.rut
--#left join p2019.temp_psu_2019 p on a.rut=p.NUMERO_DOCUMENTO
--#left join p2019.temp_psu p on a.rut=p.NUMERO_DOCUMENTO
left join p2021.ba_programa r on e.conv_codigo_carrera_demre=r.codigo_demre
left join p2021.ba_programa lp on f. lespera_codigo_carrera_demre = lp.codigo_demre
--#left join p2019.psu s on a.rut=s.NUMERO_DOCUMENTO
left join p2020.ba_psu s on a.rut=s.NUMERO_DOCUMENTO
---left join p2021.ba_postulacion v on a.rut=v.RUT and g.matricula_codigo_demre = v.CODIGO ^^^^^^^^^^^^^^^^^^^^^^^^
where a.rut is not null and a.rut<>2147483647;



#############################################################
--------------------Base Contactabilidad---------------------
#############################################################

drop table if exists p2021.ba_alumnos_contactabilidad;
create table p2020.ba_alumnos_contactabilidad as
select distinct * from (select a.rut,
       coalesce(--h.nombres,b.nombre, 
	   cast(c.nombre as string),d.nombre,--e.nombre,
	   cast(f.nombre as string),g.nombre) as nombre,
       coalesce(--h.apellido_paterno,b.apellido_paterno,
	   cast(c.apellido_paterno as string),d.apellido_materno,--e.apellido_paterno,
	   cast(f.apellido_paterno as string),g.apellido_paterno) as apellido_paterno,
       coalesce(--h.apellido_materno,b.apellido_materno,
	   cast(c.apellido_materno as string),d.apellido_materno,--e.apellido_materno,
	   cast(f.apellido_materno as string), g.apellido_materno) as apellido_materno,
       coalesce(b.post_mail, c.sim_mail,d.dif_mail,e.post_mail,f.sim_mail,g.dif_mail) as mail,
       coalesce(safe_cast(b.post_celular as int64),safe_cast(c.sim_celular as int64),safe_cast(d.dif_celular as int64),safe_cast(e.post_celular as int64),safe_cast(f.sim_celular as int64),safe_cast(g.dif_celular as int64),null) as celular,
       coalesce(safe_cast(b.post_fono_fijo as int64),safe_cast(c.sim_fono_fijo as int64),safe_cast(d.dif_fono_fijo as int64),safe_cast(e.post_fono_fijo as int64),safe_cast(f.sim_fono_fijo as int64),safe_cast(g.dif_fono_fijo as int64),null) as fono_fijo
--from p2020.ba_alumnos_p2020 a
--left join p2020.ba_alumnos_postulacion b on a.rut=b.rut
--left join p2020.ba_alumnos_simulacion_v2 c on a.rut=c.rut
--left join p2020.ba_alumnos_difusion_v2 d on a.rut=d.rut
--left join p2019.ba_alumnos_postulacion e on a.rut=e.rut
--left join p2019.ba_alumnos_simulacion_v2 f on a.rut=f.rut
--left join p2019.ba_alumnos_difusion_v2 g on a.rut=g.rut
--left join p2020.temp_matricula h on a.rut=h.rut);
from AlbertoC.ba_alumnos_p2021 a
left join AlbertoC.ba_alumnos_postulacion b on a.rut=b.rut
left join AlbertoC.ba_alumnos_simulacion_v2 c on a.rut=c.rut
left join AlbertoC.ba_alumnos_difusion_v2 d on a.rut=d.rut
left join ManuelT.ba_alumnos_postulacion e on a.rut=e.rut
left join ManuelT.ba_alumnos_simulacion_v2 f on a.rut=f.rut
left join ManuelT.ba_alumnos_difusion_v2 g on a.rut=g.rut
--left join AlbertoC.ba_alumnos_matricula h on a.rut=h.rut
);
