import pandas as pd
import numpy as np
import time
import os,sys

import warnings
warnings.filterwarnings('ignore')

import time
import os
from google.cloud import bigquery

class KBIJ(object):
    # def __init__(self):

    def _print_start_section(self, section):
        print('================================================================================')
        print(section, '\n')

    def _print_end_section(self):
        print('\n[DONE]')
        print('================================================================================')

    def get_F01_data(self, year_date, filter=True):
        client = bigquery.Client(project="athena-179008")

        filter_q=''

        if filter:
            filter_q = """AND (c.status = 'ACTIVE'
                        OR (c.status IN ('FINISHED', 'FINISHED_RESTRUCTURED') AND date(c.updatedAt) BETWEEN date_trunc(DATE_SUB(DATE({date}), INTERVAL 33 DAY), MONTH) AND date_trunc(DATE({date}), MONTH))
                        OR (c.status IN ('REJECTED', 'CANCELLED') AND date(c.updatedAt) >= date_trunc(DATE_SUB(DATE({date}), INTERVAL 33 DAY), MONTH)))
                        AND date(c.`approvedDate`) < date_trunc(DATE({date}), MONTH)""".format(date=year_date)
        # else:
        #     filter_q = """"""

        sql = """
            with user_virtual_account as (
                        select `userId`, `virtualAccountNumber`
                        from vayu.indodana_athena_virtual_accounts
                        where `virtualAccountBank` = 'BCA'
            )
            , user_virtual_account_subs AS (
                SELECT userId, virtualAccountNumber
                FROM (
                    SELECT vc.userId, virtualAccountBank, virtualAccountNumber, active, vc.createdAt, approvedDate
                            ,   row_number() over (PARTITION by vc.userId ORDER by vc.createdAt asc) as row_number
                    FROM `vayu.indodana_athena_virtual_accounts` vc
                    JOIN vayu.indodana_athena_applications a ON vc.userId = a.userId
                    JOIN vayu.indodana_athena_contracts c ON a.orderId = c.applicationOrderId
                    WHERE virtualAccountBank != 'BCA'
                            AND DATE(vc.createdAt) = DATE(approvedDate)
                    ORDER BY userId, vc.createdAt
                )
                WHERE row_number = 1
            )
            , detail_payment as (
                select applicationOrderId as orderId, amount, paymentProviderTransactionId, paymentProviderReceivedDate, up.createdAt
                from `vayu.indodana_athena_contracts` con
                left join vayu.indodana_athena_user_payments up on up.contractId = con.id and paymentProvider != 'WAIVER' and DATE(paymentProviderReceivedDate) <= date_trunc(DATE({date}), MONTH)
            )
            , count_payment as (
                select orderId
                        , SUM(CASE
                                WHEN paymentProviderTransactionId IS NOT NULL THEN 1
                                ELSE 0
                            END) as count_payment
                from detail_payment
                group by 1
            )
            , test_apps AS (
                SELECT orderId
                FROM `vayu.indodana_athena_applications`
                WHERE (applicantPersonalEmail LIKE '%@cermati%'
                        OR applicantPersonalEmail LIKE '%@indodana%'
                        OR applicantPersonalEmail = 'akoesnan2@gmail.com')
                        OR orderId LIKE '%LOP%'
                        OR orderId LIKE 'LBH%'
            )
            , summary AS (
            SELECT '0301' AS `Kode_Jenis_Pelapor`,
                    '900013' AS `Kode_Pelapor`,
                    FORMAT_DATE('%Y%m', DATE({date})) AS `Tahun_Bulan_Data`,
                    'F01' AS `Kode_Jenis_Fasilitas`,
                    a.`orderId` AS `Nomor_Rekening_Fasilitas`,
                    NULL AS `Nomor_Rekening_Lama_Fasilitas`,
                    replace(coalesce(uv.`virtualAccountNumber`, uvs.virtualAccountNumber, 'NULL'),'+','') AS `Nomor_CIF_Debitur`,
                    '9' AS `Kode_Sifat_Kredit_atau_Pembiayaan`,
                    '05' AS `Kode_Jenis_Kredit_atau_Pembiayaan`,
                    '00' AS `Kode_Akad_Kredit_atau_Akad_Pembiayaan`,
                    a.`orderId` AS `Nomor_Akad_Awal`,
                    FORMAT_DATE('%Y%m%d', date(`approvedDate`)) AS `Tanggal_Akad_Awal`,
                    a.`orderId` AS `Nomor_Akad_Akhir`,
                    FORMAT_DATE('%Y%m%d', date(`approvedDate`)) AS `Tanggal_Akad_Akhir`,
                    0 AS `Frekuensi_Perpanjangan_Fasilitas_Kredit_atau_Pembiayaan`,
                    FORMAT_DATE('%Y%m%d', date(`approvedDate`)) AS `Tanggal_Awal_Kredit_atau_Pembiayaan`,
                    FORMAT_DATE('%Y%m%d', date(IC.`startDate`)) AS `Tanggal_Mulai`,
                    FORMAT_DATE('%Y%m%d', date_add(date(cast(EXTRACT(YEAR FROM `startDate`) as INT64),CAST(EXTRACT(MONTH FROM `startDate`) as INT64),`day`), interval `approvedLoanTenureInMonth` Month)) AS `Tanggal_Jatuh_Tempo`,
                    '99' AS `Kode_Kategori_Debitur`,
                    CASE
                        WHEN A.`loanPurpose` = 'PENGEMBANGAN USAHA' THEN 2
                        ELSE 3
                    END AS `Kode_Jenis_Penggunaan`,
                    '3' AS `Kode_Orientasi_Penggunaan`,
                    '009000' AS `Kode_Sektor_Ekonomi`,
                    CASE
                        WHEN A.`addressIsCurrentAddress` = 'Ya' THEN A.`applicantResidenceCity`
                        ELSE A.`applicantCurrentResidenceCity`
                    END AS `City`,
                    CASE
                        WHEN a.`loanPurpose` = 'PENGEMBANGAN USAHA' THEN `loanAmount`
                        ELSE NULL
                    END AS `Nilai_Proyek`,
                    'IDR' AS `Kode_Valuta`,
                    0.96 AS `Suku_Bunga_atau_Imbalan`,
                    1 AS `Jenis_Suku_Bunga_atau_Imbalan`,
                    '001' AS `Kredit_atau_Pembiayaan_Program_Pemerintah`,
                    c.`approvedLoanAmount` AS `Plafon_Awal`,
                    c.`approvedLoanAmount` AS `Plafon`,
                    c.`approvedLoanAmount` AS `Realisasi_atau_Pencairan_Bulan_Berjalan`,
                    c.`lateFee` AS `Denda`,
                    CASE
                        WHEN c.`status` IN ('REJECTED',
                                            'CANCELLED','FINISHED', 'FINISHED_RESTRUCTURED')
                            THEN 0
                        ELSE CASE
                                WHEN `balance` > c.`approvedLoanAmount`
                                    THEN c.`approvedLoanAmount`
                                ELSE `balance`
                            END
                    END AS `Baki_Debet`,
                    NULL AS `Nilai_dalam_Mata_Uang_Asal`,
                    CASE
                        WHEN c.dayPastDue < 1 THEN '1'
                        WHEN c.dayPastDue <= 90 THEN '2'
                        WHEN c.dayPastDue <= 120 THEN '3'
                        WHEN c.dayPastDue <= 180 THEN '4'
                        WHEN c.dayPastDue > 180 THEN '5'
                        ELSE '1'
                    END AS `Kode_Kualitas_Kredit_atau_Pembiayaan`,
                    CASE
                        WHEN paidOffDate IS NOT NULL AND c.dayPastDue > 180 THEN FORMAT_DATE('%Y%m%d', date_add(date_sub(DATE(paidOffDate), interval c.dayPastDue DAY), interval 180 DAY))
                        WHEN c.dayPastDue > 180 THEN FORMAT_DATE('%Y%m%d', date_add(DATE(nextDueDate), interval 180 DAY))
                        ELSE NULL
                    END AS `Tanggal_Macet`,
                    CASE
                        WHEN c.dayPastDue > 180 THEN '99'
                        ELSE ''
                    END AS `Kode_Sebab_Macet`,
            --         CASE
            --             WHEN c.`status` IN ('REJECTED',
            --                                 'CANCELLED','FINISHED', 'FINISHED_RESTRUCTURED')
            --                 THEN 0
            --             ELSE `balance`
            --         END AS `Tunggakan_Pokok`,
                    CASE
                        WHEN c.`status` NOT IN ('REJECTED',
                                            'CANCELLED','FINISHED', 'FINISHED_RESTRUCTURED')
                            THEN CAST((balanceWithInterest - balance) as int64)
                        ELSE 0
                    END AS `Tunggakan_Bunga_atau_Imbalan`,
                    CASE
                        WHEN c.`status` IN ('FINISHED', 'FINISHED_RESTRUCTURED') THEN 0
                        WHEN balance = 0 THEN 0
                        WHEN c.dayPastDue > 0 THEN c.dayPastDue
                        ELSE 0
                    END AS `Jumlah_Hari_Tunggakan`,
            --         CASE
            --             WHEN c.`status` IN ('FINISHED', 'FINISHED_RESTRUCTURED') THEN 0
            --             ELSE `latePaymentCount`
            --         END AS `Frekuensi_Tunggakan`,
                    CASE
                    WHEN c.`status` IN ('FINISHED', 'FINISHED_RESTRUCTURED') THEN 0
                    WHEN c.dayPastDue > 0 THEN
                        CASE 
                        WHEN cast(date_diff(date({date}), date(approvedDate), DAY)/30 as int64) > approvedLoanTenureInMonth THEN approvedLoanTenureInMonth
                        ELSE cast(date_diff(date({date}), date(approvedDate), DAY)/30 as int64)
                        END - cp.count_payment
                    ELSE 0
                    END AS `Frekuensi_Tunggakan`,
                    0 AS `Frekuensi_Restrukturisasi`,
                    '' AS `Tanggal_Restrukturisasi_Awal`,
                    '' AS `Tanggal_Restrukturisasi_Akhir`,
                    '' AS `Kode_Cara_Restrukturisasi`,
                    CASE
                        WHEN c.`status` IN ('REJECTED',
                                            'CANCELLED') THEN '01'
                        WHEN c.`status` IN ('FINISHED', 'FINISHED_RESTRUCTURED') THEN '02'
                        ELSE '00'
                    END AS `Kode_Kondisi`,
                    CASE
                        WHEN c.`status` IN ('REJECTED',
                                            'CANCELLED','FINISHED', 'FINISHED_RESTRUCTURED') THEN c.`updatedAt`
                        ELSE NULL
                    END AS `Tanggal_Kondisi`,
                    NULL AS `Keterangan`,
                    '000' AS `Kode_Kantor_Cabang`,
                    'C' AS `Operasi_Data`,
                    'T' AS `Status_delete`,
                    FORMAT_TIMESTAMP('%Y%m%d%H%M%S', `approvedDate`) AS `Create_Date`,
                    FORMAT_TIMESTAMP('%Y%m%d%H%M%S', c.`updatedAt`) AS `Update_Date`,
                    date(c.`approvedDate`) AS Approved_Date,
                    a.userId,
                    a.orderId,
                    c.status,
                    c.dayPastDue,
                    c.approvedLoanAmount,
                    approvedLoanTenureInMonth,
                    c.maxdayPastDue,
                    c.`lateFee`,
                    c.balanceWithInterest,
                    c.balance,
                    c.dataDate,
                    CASE 
                    WHEN cast(date_diff(date({date}), date(approvedDate), DAY)/30 as int64) > approvedLoanTenureInMonth THEN approvedLoanTenureInMonth
                    ELSE cast(date_diff(date({date}), date(approvedDate), DAY)/30 as int64)
                    END as current_payment_periode_month,
                    cp.count_payment,
                    c.paidoffDate,
                    row_number() over (PARTITION by a.userId, a.orderId ORDER by replace(coalesce(CAST(uv.virtualAccountNumber AS STRING), CAST(uvs.virtualAccountNumber AS STRING), 'NULL'),'+',''), c.approvedDate desc) as row_number --used to eliminate double user_id
                    FROM vayu.indodana_athena_applications a
                    INNER JOIN `vayu_data_mart.indodana_athena_contracts` c ON a.orderId = c.applicationOrderId AND c.dataDate = date_trunc(DATE({date}), MONTH)
                    LEFT JOIN vayu.indodana_athena_installment_commitments ic ON c.`id` = ic.`contractId`
                    LEFT JOIN user_virtual_account uv ON a.`userId` = uv.`userId`
                    LEFT JOIN user_virtual_account_subs uvs on a.userId  = uvs.userId
                    LEFT JOIN count_payment cp on a.orderId = cp.orderId
                    WHERE  a.orderId NOT IN (SELECT orderId
                                            FROM test_apps)
                    {filter_q}
            )

            SELECT *
                , CAST(
                    CASE
                        WHEN Kode_Kualitas_Kredit_atau_Pembiayaan <= '1'
                            THEN 0
                        ELSE jumlah_cicilan*Frekuensi_Tunggakan
                    END as int64) AS `Tunggakan_Pokok`,
            FROM(
            SELECT
            `Kode_Jenis_Pelapor`,
            `Kode_Pelapor`,
            `Tahun_Bulan_Data`,
            `Kode_Jenis_Fasilitas`,
            `Nomor_Rekening_Fasilitas`,
            `Nomor_Rekening_Lama_Fasilitas`,
            `Nomor_CIF_Debitur`,
            `Kode_Sifat_Kredit_atau_Pembiayaan`,
            `Kode_Jenis_Kredit_atau_Pembiayaan`,
            `Kode_Akad_Kredit_atau_Akad_Pembiayaan`,
            `Nomor_Akad_Awal`,
            `Tanggal_Akad_Awal`,
            `Nomor_Akad_Akhir`,
            `Tanggal_Akad_Akhir`,
            `Frekuensi_Perpanjangan_Fasilitas_Kredit_atau_Pembiayaan`,
            `Tanggal_Awal_Kredit_atau_Pembiayaan`,
            `Tanggal_Mulai`,
            `Tanggal_Jatuh_Tempo`,
            `Kode_Kategori_Debitur`,
            `Kode_Jenis_Penggunaan`,
            `Kode_Orientasi_Penggunaan`,
            `Kode_Sektor_Ekonomi`,
            CASE
                WHEN`City` = 'PROVINSI JAWA BARAT' then '0100'
                WHEN`City` = 'KABUPATEN BEKASI' then '0102'
                WHEN`City` = 'KABUPATEN PURWAKARTA' then '0103'
                WHEN`City` = 'KABUPATEN KARAWANG' then '0106'
                WHEN`City` = 'KABUPATEN BOGOR' then '0108'
                WHEN`City` = 'BOGOR' then '0108'
                WHEN`City` = 'KABUPATEN SUKABUMI' then '0109'
                WHEN`City` = 'KABUPATEN CIANJUR' then '0110'
                WHEN`City` = 'KABUPATEN BANDUNG' then '0111'
                WHEN`City` = 'KABUPATEN SUMEDANG' then '0112'
                WHEN`City` = 'KABUPATEN TASIKMALAYA' then '0113'
                WHEN`City` = 'KABUPATEN GARUT' then '0114'
                WHEN`City` = 'KABUPATEN CIAMIS' then '0115'
                WHEN`City` = 'KABUPATEN CIREBON' then '0116'
                WHEN`City` = 'KABUPATEN KUNINGAN' then '0117'
                WHEN`City` = 'KABUPATEN INDRAMAYU' then '0118'
                WHEN`City` = 'KABUPATEN MAJALENGKA' then '0119'
                WHEN`City` = 'KABUPATEN SUBANG' then '0121'
                WHEN`City` = 'KABUPATEN BANDUNG BARAT' then '0122'
                WHEN`City` = 'KOTA BANJAR' then '0180'
                WHEN`City` = 'KOTA BANDUNG' then '0191'
                WHEN`City` = 'KOTA BOGOR' then '0192'
                WHEN`City` = 'KOTA SUKABUMI' then '0193'
                WHEN`City` = 'KOTA CIREBON' then '0194'
                WHEN`City` = 'KOTA TASIKMALAYA' then '0195'
                WHEN`City` = 'KOTA CIMAHI' then '0196'
                WHEN`City` = 'KOTA DEPOK' then '0197'
                WHEN`City` = 'KOTA BEKASI' then '0198'
                WHEN`City` = 'KABUPATEN PANGANDARAN' then '0123'
                WHEN`City` = 'PROVINSI BANTEN' then '0200'
                WHEN`City` = 'KABUPATEN LEBAK' then '0201'
                WHEN`City` = 'KABUPATEN PANDEGLANG' then '0202'
                WHEN`City` = 'KABUPATEN SERANG' then '0203'
                WHEN`City` = 'KABUPATEN TANGERANG' then '0204'
                WHEN`City` = 'KOTA CILEGON' then '0291'
                WHEN`City` = 'KOTA TANGERANG' then '0292'
                WHEN`City` = 'KOTA SERANG' then '0293'
                WHEN`City` = 'KOTA TANGERANG SELATAN' then '0294'
                WHEN`City` = 'KOTA BANTEN' then '0294'
                WHEN`City` = 'PROVINSI DKI JAYA' then '0300'
                WHEN`City` = 'KOTA JAKARTA PUSAT' then '0391'
                WHEN`City` = 'KOTA JAKARTA UTARA' then '0392'
                WHEN`City` = 'KOTA JAKARTA BARAT' then '0393'
                WHEN`City` = 'KOTA JAKARTA SELATAN' then '0394'
                WHEN`City` = 'KOTA ADM. JAKARTA SELATAN' then '0394'
                WHEN`City` = 'KOTA JAKARTA TIMUR' then '0395'
                WHEN`City` = 'KEPULAUAN SERIBU' then '0396'
                WHEN`City` = 'KABUPATEN KEPULAUAN SERIBU' then '0396'
                WHEN`City` = 'DAERAH ISTIMEWA YOGYAKARTA' then '0500'
                WHEN`City` = 'KABUPATEN BANTUL' then '0501'
                WHEN`City` = 'KABUPATEN SLEMAN' then '0502'
                WHEN`City` = 'KABUPATEN GUNUNG KIDUL' then '0503'
                WHEN`City` = 'KABUPATEN KULON PROGO' then '0504'
                WHEN`City` = 'KOTA YOGYAKARTA' then '0591'
                WHEN`City` = 'KABUPATEN PEKALONGAN' then '0905'
                WHEN`City` = 'KABUPATEN TEGAL' then '0906'
                WHEN`City` = 'KABUPATEN BREBES' then '0907'
                WHEN`City` = 'KABUPATEN PATI' then '0908'
                WHEN`City` = 'KABUPATEN KUDUS' then '0909'
                WHEN`City` = 'KABUPATEN PEMALANG' then '0910'
                WHEN`City` = 'KABUPATEN JEPARA' then '0911'
                WHEN`City` = 'KABUPATEN REMBANG' then '0912'
                WHEN`City` = 'KABUPATEN BLORA' then '0913'
                WHEN`City` = 'KABUPATEN BANYUMAS' then '0914'
                WHEN`City` = 'KABUPATEN CILACAP' then '0915'
                WHEN`City` = 'KABUPATEN PURBALINGGA' then '0916'
                WHEN`City` = 'KABUPATEN BANJARNEGARA' then '0917'
                WHEN`City` = 'KABUPATEN MAGELANG' then '0918'
                WHEN`City` = 'KABUPATEN TEMANGGUNG' then '0919'
                WHEN`City` = 'KABUPATEN WONOSOBO' then '0920'
                WHEN`City` = 'KABUPATEN PURWOREJO' then '0921'
                WHEN`City` = 'KABUPATEN KEBUMEN' then '0922'
                WHEN`City` = 'KABUPATEN KLATEN' then '0923'
                WHEN`City` = 'KABUPATEN BOYOLALI' then '0924'
                WHEN`City` = 'KABUPATEN SRAGEN' then '0925'
                WHEN`City` = 'KABUPATEN SUKOHARJO' then '0926'
                WHEN`City` = 'KABUPATEN KARANGANYAR' then '0927'
                WHEN`City` = 'KABUPATEN WONOGIRI' then '0928'
                WHEN`City` = 'KABUPATEN BATANG' then '0929'
                WHEN`City` = 'KOTA SEMARANG' then '0991'
                WHEN`City` = 'KOTA SALATIGA' then '0992'
                WHEN`City` = 'KOTA PEKALONGAN' then '0993'
                WHEN`City` = 'KOTA TEGAL' then '0994'
                WHEN`City` = 'KOTA MAGELANG' then '0995'
                WHEN`City` = 'KOTA SURAKARTA' then '0996'
                WHEN`City` = 'KOTA SOLO' then '0996'
                WHEN`City` = 'KABUPATEN GROBOGAN' then '0904'
                WHEN`City` = 'PROVINSI JAWA TENGAH' then '0900'
                WHEN`City` = 'KABUPATEN SEMARANG' then '0901'
                WHEN`City` = 'KABUPATEN KENDAL' then '0902'
                WHEN`City` = 'KABUPATEN DEMAK' then '0903'
                WHEN`City` = 'PROVINSI JAWA TIMUR' then '1200'
                WHEN`City` = 'KABUPATEN GRESIK' then '1201'
                WHEN`City` = 'KABUPATEN SIDOARJO' then '1202'
                WHEN`City` = 'KABUPATEN MOJOKERTO' then '1203'
                WHEN`City` = 'KABUPATEN JOMBANG' then '1204'
                WHEN`City` = 'KABUPATEN SAMPANG' then '1205'
                WHEN`City` = 'KABUPATEN PAMEKASAN' then '1206'
                WHEN`City` = 'KABUPATEN SUMENEP' then '1207'
                WHEN`City` = 'KABUPATEN BANGKALAN' then '1208'
                WHEN`City` = 'KABUPATEN BONDOWOSO' then '1209'
                WHEN`City` = 'KABUPATEN BANYUWANGI' then '1211'
                WHEN`City` = 'KABUPATEN JEMBER' then '1212'
                WHEN`City` = 'KABUPATEN MALANG' then '1213'
                WHEN`City` = 'KABUPATEN PASURUAN' then '1214'
                WHEN`City` = 'KABUPATEN PROBOLINGGO' then '1215'
                WHEN`City` = 'KABUPATEN LUMAJANG' then '1216'
                WHEN`City` = 'KABUPATEN KEDIRI' then '1217'
                WHEN`City` = 'KABUPATEN NGANJUK' then '1218'
                WHEN`City` = 'KABUPATEN TULUNGAGUNG' then '1219'
                WHEN`City` = 'KABUPATEN TRENGGALEK' then '1220'
                WHEN`City` = 'KABUPATEN BLITAR' then '1221'
                WHEN`City` = 'KABUPATEN MADIUN' then '1222'
                WHEN`City` = 'KABUPATEN NGAWI' then '1223'
                WHEN`City` = 'KABUPATEN MAGETAN' then '1224'
                WHEN`City` = 'KABUPATEN PONOROGO' then '1225'
                WHEN`City` = 'KABUPATEN PACITAN' then '1226'
                WHEN`City` = 'KABUPATEN BOJONEGORO' then '1227'
                WHEN`City` = 'KABUPATEN TUBAN' then '1228'
                WHEN`City` = 'KABUPATEN LAMONGAN' then '1229'
                WHEN`City` = 'KABUPATEN SITUBONDO' then '1230'
                WHEN`City` = 'KOTA BATU' then '1271'
                WHEN`City` = 'KOTA SURABAYA' then '1291'
                WHEN`City` = 'KOTA MOJOKERTO' then '1292'
                WHEN`City` = 'KOTA MALANG' then '1293'
                WHEN`City` = 'KOTA PASURUAN' then '1294'
                WHEN`City` = 'KOTA PROBOLINGGO' then '1295'
                WHEN`City` = 'KOTA BLITAR' then '1296'
                WHEN`City` = 'KOTA KEDIRI' then '1297'
                WHEN`City` = 'KOTA MADIUN' then '1298'
                WHEN`City` = 'PROVINSI BENGKULU' then '2300'
                WHEN`City` = 'KABUPATEN BENGKULU SELATAN' then '2301'
                WHEN`City` = 'KABUPATEN BENGKULU UTARA' then '2302'
                WHEN`City` = 'KABUPATEN REJANG LEBONG' then '2303'
                WHEN`City` = 'KABUPATEN LEBONG' then '2304'
                WHEN`City` = 'KABUPATEN KEPAHIANG' then '2305'
                WHEN`City` = 'KABUPATEN MUKOMUKO' then '2306'
                WHEN`City` = 'KABUPATEN SELUMA' then '2307'
                WHEN`City` = 'KABUPATEN KAUR' then '2308'
                WHEN`City` = 'KABUPATEN BENGKULU TENGAH' then '2309'
                WHEN`City` = 'KOTA BENGKULU' then '2391'
                WHEN`City` = 'PROVINSI JAMBI' then '3100'
                WHEN`City` = 'KABUPATEN BATANGHARI' then '3101'
                WHEN`City` = 'KABUPATEN SAROLANGUN' then '3104'
                WHEN`City` = 'KABUPATEN KERINCI' then '3105'
                WHEN`City` = 'KABUPATEN MUARO JAMBI' then '3106'
                WHEN`City` = 'KABUPATEN TANJUNG JABUNG BARAT' then '3107'
                WHEN`City` = 'KABUPATEN TANJUNG JABUNG TIMUR' then '3108'
                WHEN`City` = 'KABUPATEN TEBO' then '3109'
                WHEN`City` = 'KABUPATEN MERANGIN' then '3111'
                WHEN`City` = 'KABUPATEN BUNGO' then '3112'
                WHEN`City` = 'KOTA JAMBI' then '3191'
                WHEN`City` = 'KOTA SUNGAI PENUH' then '3192'
                WHEN`City` = 'PROVINSI NAD' then '3200'
                WHEN`City` = 'KABUPATEN ACEH BESAR' then '3201'
                WHEN`City` = 'KABUPATEN PIDIE' then '3202'
                WHEN`City` = 'KABUPATEN ACEH UTARA' then '3203'
                WHEN`City` = 'KABUPATEN ACEH TIMUR' then '3204'
                WHEN`City` = 'KABUPATEN ACEH SELATAN' then '3205'
                WHEN`City` = 'KABUPATEN ACEH BARAT' then '3206'
                WHEN`City` = 'KABUPATEN ACEH TENGAH' then '3207'
                WHEN`City` = 'KABUPATEN ACEH TENGGARA' then '3208'
                WHEN`City` = 'KABUPATEN ACEH SINGKIL' then '3209'
                WHEN`City` = 'KABUPATEN ACEH JEUMPA /BIREUEN' then '3210'
                WHEN`City` = 'KABUPATEN ACEH TAMIANG' then '3211'
                WHEN`City` = 'KABUPATEN GAYO LUWES' then '3212'
                WHEN`City` = 'KABUPATEN ACEH BARAT DAYA' then '3213'
                WHEN`City` = 'KABUPATEN ACEH JAYA' then '3214'
                WHEN`City` = 'KABUPATEN NAGAN RAYA' then '3215'
                WHEN`City` = 'KABUPATEN SIMEULEU' then '3216'
                WHEN`City` = 'KABUPATEN BENER MERIAH' then '3217'
                WHEN`City` = 'KABUPATEN PIDIE JAYA' then '3218'
                WHEN`City` = 'KABUPATEN SUBULUSSALAM' then '3219'
                WHEN`City` = 'KOTA BANDA ACEH' then '3291'
                WHEN`City` = 'KOTA SABANG' then '3292'
                WHEN`City` = 'KOTA LHOKSEUMAWE' then '3293'
                WHEN`City` = 'KOTA LANGSA' then '3294'
                WHEN`City` = 'PROVINSI SUMATERA UTARA' then '3300'
                WHEN`City` = 'KABUPATEN DELI SERDANG' then '3301'
                WHEN`City` = 'KABUPATEN LANGKAT' then '3302'
                WHEN`City` = 'KABUPATEN KARO' then '3303'
                WHEN`City` = 'KABUPATEN SIMALUNGUN' then '3304'
                WHEN`City` = 'KABUPATEN LABUHAN BATU' then '3305'
                WHEN`City` = 'KABUPATEN ASAHAN' then '3306'
                WHEN`City` = 'KABUPATEN DAIRI' then '3307'
                WHEN`City` = 'KABUPATEN TAPANULI UTARA' then '3308'
                WHEN`City` = 'KABUPATEN TAPANULI TENGAH' then '3309'
                WHEN`City` = 'KABUPATEN TAPANULI SELATAN' then '3310'
                WHEN`City` = 'KABUPATEN NIAS' then '3311'
                WHEN`City` = 'KABUPATEN TOBA SAMOSIR' then '3313'
                WHEN`City` = 'KABUPATEN MANDAILING NATAL' then '3314'
                WHEN`City` = 'KABUPATEN NIAS SELATAN' then '3315'
                WHEN`City` = 'KABUPATEN HUMBANG HASUNDUTAN' then '3316'
                WHEN`City` = 'KABUPATEN PAKPAK BHARAT' then '3317'
                WHEN`City` = 'KABUPATEN SAMOSIR' then '3318'
                WHEN`City` = 'KABUPATEN SERDANG BEDAGAI' then '3319'
                WHEN`City` = 'KABUPATEN BATU BARA' then '3321'
                WHEN`City` = 'KABUPATEN PADANG LAWAS' then '3322'
                WHEN`City` = 'KABUPATEN PADANG LAWAS UTARA' then '3323'
                WHEN`City` = 'KABUPATEN LABUANBATU SELATAN' then '3324'
                WHEN`City` = 'KABUPATEN LABUANBATU UTARA' then '3325'
                WHEN`City` = 'KABUPATEN NIAS BARAT' then '3326'
                WHEN`City` = 'KABUPATEN NIAS UTARA' then '3327'
                WHEN`City` = 'KOTA TEBING TINGGI' then '3391'
                WHEN`City` = 'KOTA BINJAI' then '3392'
                WHEN`City` = 'KOTA PEMATANG SIANTAR' then '3393'
                WHEN`City` = 'KOTA TANJUNG BALAI' then '3394'
                WHEN`City` = 'KOTA SIBOLGA' then '3395'
                WHEN`City` = 'KOTA MEDAN' then '3396'
                WHEN`City` = 'KOTA GUNUNG SITOLI' then '3397'
                WHEN`City` = 'KOTA PADANG SIDEMPUAN' then '3399'
                WHEN`City` = 'PROVINSI SUMATERA BARAT' then '3400'
                WHEN`City` = 'KABUPATEN AGAM' then '3401'
                WHEN`City` = 'KABUPATEN PASAMAN' then '3402'
                WHEN`City` = 'KABUPATEN LIMAPULUH KOTA' then '3403'
                WHEN`City` = 'KABUPATEN SOLOK SELATAN' then '3404'
                WHEN`City` = 'KABUPATEN PADANG PARIAMAN' then '3405'
                WHEN`City` = 'KABUPATEN PESISIR SELATAN' then '3406'
                WHEN`City` = 'KABUPATEN TANAH DATAR' then '3407'
                WHEN`City` = 'KABUPATEN SAWAH LUNTO/SIJUNJUNG' then '3408'
                WHEN`City` = 'KABUPATEN KEPULAUAN MENTAWAI' then '3409'
                WHEN`City` = 'KABUPATEN PASAMAN BARAT' then '3410'
                WHEN`City` = 'KABUPATEN DHARMASRAYA' then '3411'
                WHEN`City` = 'KABUPATEN SOLOK' then '3412'
                WHEN`City` = 'KOTA BUKITTINGGI' then '3491'
                WHEN`City` = 'KOTA PADANG' then '3492'
                WHEN`City` = 'KOTA SAWAHLUNTO' then '3493'
                WHEN`City` = 'KOTA PADANG PANJANG' then '3494'
                WHEN`City` = 'KOTA SOLOK' then '3495'
                WHEN`City` = 'KOTA PAYAKUMBUH' then '3496'
                WHEN`City` = 'KOTA PARIAMAN' then '3497'
                WHEN`City` = 'PROVINSI RIAU' then '3500'
                WHEN`City` = 'KABUPATEN KAMPAR' then '3501'
                WHEN`City` = 'KABUPATEN BENGKALIS' then '3502'
                WHEN`City` = 'KABUPATEN INDRAGIRI HULU' then '3504'
                WHEN`City` = 'KABUPATEN INDRAGIRI HILIR' then '3505'
                WHEN`City` = 'KABUPATEN ROKAN HULU' then '3508'
                WHEN`City` = 'KABUPATEN ROKAN HILIR' then '3509'
                WHEN`City` = 'KABUPATEN PELALAWAN' then '3510'
                WHEN`City` = 'KABUPATEN SIAK' then '3511'
                WHEN`City` = 'KABUPATEN KUANTAN SINGINGI' then '3512'
                WHEN`City` = 'KABUPATEN KEPULAUAN MERANTI' then '3513'
                WHEN`City` = 'KOTA PEKANBARU' then '3591'
                WHEN`City` = 'KOTA DUMAI' then '3592'
                WHEN`City` = 'PROVINSI SUMATERA SELATAN' then '3600'
                WHEN`City` = 'KABUPATEN MUSI BANYUASIN' then '3606'
                WHEN`City` = 'KABUPATEN OGAN KOMERING ULU' then '3607'
                WHEN`City` = 'KABUPATEN MUARA ENIM' then '3608'
                WHEN`City` = 'KABUPATEN LAHAT' then '3609'
                WHEN`City` = 'KABUPATEN MUSI RAWAS' then '3610'
                WHEN`City` = 'KABUPATEN OGAN KOMERING ILIR' then '3611'
                WHEN`City` = 'KABUPATEN BANYUASIN' then '3613'
                WHEN`City` = 'KABUPATEN OGAN KOMERING ULU SELATAN' then '3614'
                WHEN`City` = 'KABUPATEN OGAN KOMERING ULU TIMUR' then '3615'
                WHEN`City` = 'KABUPATEN OGAN ILIR' then '3616'
                WHEN`City` = 'KABUPATEN PENUKAL ABAB LEMATANG ILIR' then '3619'
                WHEN`City` = 'KOTA PALEMBANG' then '3691'
                WHEN`City` = 'KOTA LUBUKLINGGAU' then '3693'
                WHEN`City` = 'KOTA PRABUMULIH' then '3694'
                WHEN`City` = 'KOTA PAGAR ALAM' then '3697'
                WHEN`City` = 'KABUPATEN MUSI RAWAS UTARA' then '3618'
                WHEN`City` = 'KABUPATEN EMPAT LAWANG' then '3617'
                WHEN`City` = 'PROVINSI KEPULAUAN BANGKA BELITUNG' then '3700'
                WHEN`City` = 'KABUPATEN BANGKA' then '3701'
                WHEN`City` = 'KABUPATEN BELITUNG' then '3702'
                WHEN`City` = 'KABUPATEN BANGKA BARAT' then '3703'
                WHEN`City` = 'KABUPATEN BANGKA SELATAN' then '3704'
                WHEN`City` = 'KABUPATEN BANGKA TENGAH' then '3705'
                WHEN`City` = 'KABUPATEN BELITUNG TIMUR' then '3706'
                WHEN`City` = 'KABUPATEN BANGKA BELITUNG(LBU 2016)/KOTA PANGKAL PINANG' then '3707'
                WHEN`City` = 'KOTA PANGKAL PINANG' then '3791'
                WHEN`City` = 'PROVINSI KEPULAUAN RIAU' then '3800'
                WHEN`City` = 'KABUPATEN KARIMUN' then '3801'
                WHEN`City` = 'KABUPATEN LINGGA' then '3802'
                WHEN`City` = 'KABUPATEN NATUNA' then '3803'
                WHEN`City` = 'KABUPATEN BINTAN' then '3804'
                WHEN`City` = 'KABUPATEN KEPULAUAN ANAMBAS' then '3805'
                WHEN`City` = 'KOTA TANJUNG PINANG' then '3891'
                WHEN`City` = 'KOTA BATAM' then '3892'
                WHEN`City` = 'PROVINSI LAMPUNG' then '3900'
                WHEN`City` = 'KABUPATEN LAMPUNG SELATAN' then '3901'
                WHEN`City` = 'KABUPATEN LAMPUNG TENGAH' then '3902'
                WHEN`City` = 'KABUPATEN LAMPUNG UTARA' then '3903'
                WHEN`City` = 'KABUPATEN LAMPUNG BARAT' then '3904'
                WHEN`City` = 'KABUPATEN TULANG BAWANG' then '3905'
                WHEN`City` = 'KABUPATEN TANGGAMUS' then '3906'
                WHEN`City` = 'KABUPATEN LAMPUNG TIMUR' then '3907'
                WHEN`City` = 'KABUPATEN WAY KANAN' then '3908'
                WHEN`City` = 'KABUPATEN PESAWARAN' then '3909'
                WHEN`City` = 'KABUPATEN PRINGSEWU' then '3910'
                WHEN`City` = 'KABUPATEN TULANG BAWANG BARAT' then '3911'
                WHEN`City` = 'KABUPATEN PESISIR BARAT' then '3913'
                WHEN`City` = 'KOTA BANDAR LAMPUNG' then '3991'
                WHEN`City` = 'KOTA METRO' then '3992'
                WHEN`City` = 'KABUPATEN MESUJI' then '3912'
                WHEN`City` = 'KABUPATEN BANJAR' then '5101'
                WHEN`City` = 'KABUPATEN TANAH LAUT' then '5102'
                WHEN`City` = 'KABUPATEN TAPIN' then '5103'
                WHEN`City` = 'KABUPATEN HULU SUNGAI SELATAN' then '5104'
                WHEN`City` = 'KABUPATEN HULU SUNGAI TENGAH' then '5105'
                WHEN`City` = 'KABUPATEN HULU SUNGAI UTARA' then '5106'
                WHEN`City` = 'KABUPATEN BARITO KUALA' then '5107'
                WHEN`City` = 'KABUPATEN KOTA BARU' then '5108'
                WHEN`City` = 'KABUPATEN TABALONG' then '5109'
                WHEN`City` = 'KABUPATEN TANAH BUMBU' then '5110'
                WHEN`City` = 'KABUPATEN BALANGAN' then '5111'
                WHEN`City` = 'KOTA BANJARMASIN' then '5191'
                WHEN`City` = 'KOTA BANJARBARU' then '5192'
                WHEN`City` = 'PROVINSI KALIMANTAN SELATAN' then '5100'
                WHEN`City` = 'PROVINSI KALIMANTAN BARAT' then '5300'
                WHEN`City` = 'KABUPATEN PONTIANAK' then '5301'
                WHEN`City` = 'KABUPATEN SAMBAS' then '5302'
                WHEN`City` = 'KABUPATEN KETAPANG' then '5303'
                WHEN`City` = 'KABUPATEN SANGGAU' then '5304'
                WHEN`City` = 'KABUPATEN SINTANG' then '5305'
                WHEN`City` = 'KABUPATEN KAPUAS HULU' then '5306'
                WHEN`City` = 'KABUPATEN BENGKAYANG' then '5307'
                WHEN`City` = 'KABUPATEN LANDAK' then '5308'
                WHEN`City` = 'KABUPATEN SEKADAU' then '5309'
                WHEN`City` = 'KABUPATEN MELAWI' then '5310'
                WHEN`City` = 'KABUPATEN KAYONG UTARA' then '5311'
                WHEN`City` = 'KABUPATEN KUBU RAYA' then '5312'
                WHEN`City` = 'KOTA PONTIANAK' then '5391'
                WHEN`City` = 'KOTA SINGKAWANG' then '5392'
                WHEN`City` = 'PROVINSI KALIMANTAN TIMUR' then '5400'
                WHEN`City` = 'KABUPATEN KUTAI KARTANEGARA' then '5401'
                WHEN`City` = 'KABUPATEN BERAU' then '5402'
                WHEN`City` = 'KABUPATEN PASER' then '5403'
                WHEN`City` = 'KABUPATEN BULUNGAN' then '5404'
                WHEN`City` = 'KABUPATEN KUTAI BARAT' then '5405'
                WHEN`City` = 'KABUPATEN KUTAI TIMUR' then '5406'
                WHEN`City` = 'KABUPATEN NUNUKAN' then '5409'
                WHEN`City` = 'KABUPATEN MALINAU' then '5410'
                WHEN`City` = 'KABUPATEN PENAJAM PASER UTARA' then '5411'
                WHEN`City` = 'KABUPATEN MAHAKAM ULU' then '5413'
                WHEN`City` = 'KOTA SAMARINDA' then '5491'
                WHEN`City` = 'KOTA BALIKPAPAN' then '5492'
                WHEN`City` = 'KOTA TARAKAN' then '5493'
                WHEN`City` = 'KOTA BONTANG' then '5494'
                WHEN`City` = 'KABUPATEN TANA TIDUNG' then '5412'
                WHEN`City` = 'PROVINSI KALIMANTAN TENGAH' then '5800'
                WHEN`City` = 'KABUPATEN KAPUAS' then '5801'
                WHEN`City` = 'KABUPATEN KOTAWARINGIN BARAT' then '5802'
                WHEN`City` = 'KABUPATEN KOTAWARINGIN TIMUR' then '5803'
                WHEN`City` = 'KABUPATEN MURUNG RAYA' then '5804'
                WHEN`City` = 'KABUPATEN BARITO TIMUR' then '5805'
                WHEN`City` = 'KABUPATEN BARITO SELATAN' then '5806'
                WHEN`City` = 'KABUPATEN GUNUNG MAS' then '5807'
                WHEN`City` = 'KABUPATEN BARITO UTARA' then '5808'
                WHEN`City` = 'KABUPATEN PULANG PISAU' then '5809'
                WHEN`City` = 'KABUPATEN SERUYAN' then '5810'
                WHEN`City` = 'KABUPATEN KATINGAN' then '5811'
                WHEN`City` = 'KABUPATEN SUKAMARA' then '5812'
                WHEN`City` = 'KABUPATEN LAMANDAU' then '5813'
                WHEN`City` = 'KOTA PALANGKARAYA' then '5892'
                WHEN`City` = 'PROVINSI SULAWESI TENGAH' then '6000'
                WHEN`City` = 'KABUPATEN DONGGALA' then '6001'
                WHEN`City` = 'KABUPATEN POSO' then '6002'
                WHEN`City` = 'KABUPATEN BANGGAI' then '6003'
                WHEN`City` = 'KABUPATEN TOLI-TOLI' then '6004'
                WHEN`City` = 'KABUPATEN BANGGAI KEPULAUAN' then '6005'
                WHEN`City` = 'KABUPATEN MOROWALI' then '6006'
                WHEN`City` = 'KABUPATEN BUOL' then '6007'
                WHEN`City` = 'KABUPATEN TOJO UNA-UNA' then '6008'
                WHEN`City` = 'KABUPATEN PARIGI MOUTONG' then '6009'
                WHEN`City` = 'KABUPATEN SIGI' then '6010'
                WHEN`City` = 'KOTA PALU' then '6091'
                WHEN`City` = 'KABUPATEN MOROWALI UTARA' then '6012'
                WHEN`City` = 'KABUPATEN BANGGAI LAUT' then '6011'
                WHEN`City` = 'PROVINSI SULAWESI SELATAN' then '6100'
                WHEN`City` = 'KABUPATEN PINRANG' then '6101'
                WHEN`City` = 'KABUPATEN GOWA' then '6102'
                WHEN`City` = 'KABUPATEN WAJO' then '6103'
                WHEN`City` = 'KABUPATEN BONE' then '6105'
                WHEN`City` = 'KABUPATEN TANA TORAJA' then '6106'
                WHEN`City` = 'KABUPATEN MAROS' then '6107'
                WHEN`City` = 'KABUPATEN LUWU' then '6109'
                WHEN`City` = 'KABUPATEN SINJAI' then '6110'
                WHEN`City` = 'KABUPATEN BULUKUMBA' then '6111'
                WHEN`City` = 'KABUPATEN BANTAENG' then '6112'
                WHEN`City` = 'KABUPATEN JENEPONTO' then '6113'
                WHEN`City` = 'KABUPATEN KEPULAUAN SELAYAR' then '6114'
                WHEN`City` = 'KABUPATEN TAKALAR' then '6115'
                WHEN`City` = 'KABUPATEN BARRU' then '6116'
                WHEN`City` = 'KABUPATEN SIDENRENG RAPPANG' then '6117'
                WHEN`City` = 'KABUPATEN PANGKAJENE KEPULAUAN' then '6118'
                WHEN`City` = 'KABUPATEN SOPPENG' then '6119'
                WHEN`City` = 'KABUPATEN ENREKANG' then '6121'
                WHEN`City` = 'KABUPATEN LUWU TIMUR' then '6122'
                WHEN`City` = 'KABUPATEN LUWU UTARA' then '6124'
                WHEN`City` = 'KABUPATEN TORAJA UTARA' then '6125'
                WHEN`City` = 'KOTA MAKASSAR' then '6191'
                WHEN`City` = 'KOTA PARE-PARE' then '6192'
                WHEN`City` = 'KOTA PALOPO' then '6193'
                WHEN`City` = 'PROVINSI SULAWESI UTARA' then '6200'
                WHEN`City` = 'KABUPATEN MINAHASA' then '6202'
                WHEN`City` = 'KABUPATEN BOLAANG MONGONDOW' then '6203'
                WHEN`City` = 'KABUPATEN KEPULAUAN SANGIHE' then '6204'
                WHEN`City` = 'KABUPATEN KEPULAUAN TALAUD' then '6205'
                WHEN`City` = 'KABUPATEN MINAHASA SELATAN' then '6206'
                WHEN`City` = 'KABUPATEN MINAHASA UTARA' then '6207'
                WHEN`City` = 'KABUPATEN MINAHASA TENGGARA' then '6209'
                WHEN`City` = 'KABUPATEN BOLAANG MONGONDOW UTARA' then '6210'
                WHEN`City` = 'KABUPATEN KEPULAUAN SITARO' then '6211'
                WHEN`City` = 'KABUPATEN BOLAANG MONGONDOW SELATAN' then '6212'
                WHEN`City` = 'KABUPATEN BOLAANG MONGONDOW TIMUR' then '6213'
                WHEN`City` = 'KOTA MANADO' then '6291'
                WHEN`City` = 'KOTA KOTAMOBAGU' then '6292'
                WHEN`City` = 'KOTA BITUNG' then '6293'
                WHEN`City` = 'KOTA TOMOHON' then '6294'
                WHEN`City` = 'PROVINSI GORONTALO' then '6300'
                WHEN`City` = 'KABUPATEN GORONTALO' then '6301'
                WHEN`City` = 'KABUPATEN BUALEMO' then '6302'
                WHEN`City` = 'KABUPATEN BONEBOLANGO' then '6303'
                WHEN`City` = 'KABUPATEN POHUWATO' then '6304'
                WHEN`City` = 'KABUPATEN GORONTALO UTARA' then '6305'
                WHEN`City` = 'KOTA GORONTALO' then '6391'
                WHEN`City` = 'PROVINSI SULAWESI BARAT' then '6400'
                WHEN`City` = 'KABUPATEN POLEWALI MANDAR' then '6401'
                WHEN`City` = 'KABUPATEN MAJENE' then '6402'
                WHEN`City` = 'KABUPATEN MAMASA' then '6403'
                WHEN`City` = 'KABUPATEN MAMUJU TENGAH' then '6405'
                WHEN`City` = 'KAB MAMUJU' then '6491'
                WHEN`City` = 'KABUPATEN MAMUJU UTARA' then '6404'
                WHEN`City` = 'KABUPATEN MAMUJU (LBU 2016)' then '6406'
                WHEN`City` = 'PROVINSI SULAWESI TENGGARA' then '6900'
                WHEN`City` = 'KABUPATEN BUTON' then '6901'
                WHEN`City` = 'KABUPATEN MUNA' then '6903'
                WHEN`City` = 'KABUPATEN KOLAKA' then '6904'
                WHEN`City` = 'KABUPATEN WAKATOBI' then '6905'
                WHEN`City` = 'KABUPATEN KONAWE' then '6906'
                WHEN`City` = 'KABUPATEN KONAWE SELATAN' then '6907'
                WHEN`City` = 'KABUPATEN BOMBANA' then '6908'
                WHEN`City` = 'KABUPATEN KOLAKA UTARA' then '6909'
                WHEN`City` = 'KABUPATEN BUTON UTARA' then '6910'
                WHEN`City` = 'KABUPATEN MUNA BARAT' then '6916'
                WHEN`City` = 'KOTA BAU-BAU' then '6990'
                WHEN`City` = 'KOTA KENDARI' then '6991'
                WHEN`City` = 'KABUPATEN KALOKA TIMUR' then '6912'
                WHEN`City` = 'KABUPATEN BUTON TENGAH' then '6915'
                WHEN`City` = 'KABUPATEN BUTON SELATAN' then '6914'
                WHEN`City` = 'KABUPATEN KONAWE UTARA' then '6911'
                WHEN`City` = 'KABUPATEN KONAWE KEPULAUAN' then '6913'
                WHEN`City` = 'PROVINSI NUSA TENGGARA BARAT' then '7100'
                WHEN`City` = 'KABUPATEN LOMBOK BARAT' then '7101'
                WHEN`City` = 'KABUPATEN LOMBOK TENGAH' then '7102'
                WHEN`City` = 'KABUPATEN LOMBOK TIMUR' then '7103'
                WHEN`City` = 'KABUPATEN SUMBAWA' then '7104'
                WHEN`City` = 'KABUPATEN BIMA' then '7105'
                WHEN`City` = 'KABUPATEN DOMPU' then '7106'
                WHEN`City` = 'KABUPATEN SUMBAWA BARAT' then '7107'
                WHEN`City` = 'KABUPATEN LOMBOK UTARA' then '7108'
                WHEN`City` = 'KOTA MATARAM' then '7191'
                WHEN`City` = 'KOTA BIMA' then '7192'
                WHEN`City` = 'PROVINSI BALI' then '7200'
                WHEN`City` = 'KABUPATEN BULELENG' then '7201'
                WHEN`City` = 'KABUPATEN JEMBRANA' then '7202'
                WHEN`City` = 'KABUPATEN TABANAN' then '7203'
                WHEN`City` = 'KABUPATEN BADUNG' then '7204'
                WHEN`City` = 'KABUPATEN GIANYAR' then '7205'
                WHEN`City` = 'KABUPATEN KLUNGKUNG' then '7206'
                WHEN`City` = 'KABUPATEN BANGLI' then '7207'
                WHEN`City` = 'KABUPATEN KARANGASEM' then '7208'
                WHEN`City` = 'KOTA DENPASAR' then '7291'
                WHEN`City` = 'PROVINSI NUSA TENGGARA TIMUR' then '7400'
                WHEN`City` = 'KABUPATEN KUPANG' then '7401'
                WHEN`City` = 'KABUPATEN TIMOR-TENGAH SELATAN' then '7402'
                WHEN`City` = 'KABUPATEN TIMOR-TENGAH UTARA' then '7403'
                WHEN`City` = 'KABUPATEN BELU' then '7404'
                WHEN`City` = 'KABUPATEN ALOR' then '7405'
                WHEN`City` = 'KABUPATEN FLORES TIMUR' then '7406'
                WHEN`City` = 'KABUPATEN SIKKA' then '7407'
                WHEN`City` = 'KABUPATEN ENDE' then '7408'
                WHEN`City` = 'KABUPATEN NGADA' then '7409'
                WHEN`City` = 'KABUPATEN MANGGARAI' then '7410'
                WHEN`City` = 'KABUPATEN SUMBA TIMUR' then '7411'
                WHEN`City` = 'KABUPATEN SUMBA BARAT' then '7412'
                WHEN`City` = 'KABUPATEN LEMBATA' then '7413'
                WHEN`City` = 'KABUPATEN ROTE NDAO' then '7414'
                WHEN`City` = 'KABUPATEN MANGGARAI BARAT' then '7415'
                WHEN`City` = 'KABUPATEN SUMBA TENGAH' then '7416'
                WHEN`City` = 'KABUPATEN SUMBA BARAT DAYA' then '7417'
                WHEN`City` = 'KABUPATEN MANGGARAI TIMUR' then '7418'
                WHEN`City` = 'KABUPATEN NAGEKEO' then '7419'
                WHEN`City` = 'KABUPATEN MALAKA' then '7421'
                WHEN`City` = 'KOTA KUPANG' then '7491'
                WHEN`City` = 'KABUPATEN SABU RAIJUA' then '7420'
                WHEN`City` = 'PROVINSI MALUKU' then '8100'
                WHEN`City` = 'KABUPATEN MALUKU TENGAH' then '8101'
                WHEN`City` = 'KABUPATEN MALUKU TENGGARA' then '8102'
                WHEN`City` = 'KABUPATEN MALUKU TENGGARA BARAT' then '8103'
                WHEN`City` = 'KABUPATEN BURU' then '8104'
                WHEN`City` = 'KABUPATEN SERAM BAGIAN BARAT' then '8105'
                WHEN`City` = 'KABUPATEN SERAM BAGIAN TIMUR' then '8106'
                WHEN`City` = 'KABUPATEN KEPULAUAN ARU' then '8107'
                WHEN`City` = 'KABUPATEN MALUKU BARAT DAYA' then '8108'
                WHEN`City` = 'KABUPATEN BURU SELATAN' then '8109'
                WHEN`City` = 'KOTA AMBON' then '8191'
                WHEN`City` = 'KOTA TUAL' then '8192'
                WHEN`City` = 'PROVINSI PAPUA' then '8200'
                WHEN`City` = 'KABUPATEN JAYAPURA' then '8201'
                WHEN`City` = 'KABUPATEN BIAK NUMFOR' then '8202'
                WHEN`City` = 'KABUPATEN KEPULAUAN YAPEN-WAROPEN' then '8210'
                WHEN`City` = 'KABUPATEN MERAUKE' then '8211'
                WHEN`City` = 'KABUPATEN PANIAI' then '8212'
                WHEN`City` = 'KABUPATEN JAYAWIJAYA' then '8213'
                WHEN`City` = 'KABUPATEN NABIRE' then '8214'
                WHEN`City` = 'KABUPATEN MIMIKA' then '8215'
                WHEN`City` = 'KABUPATEN PUNCAK JAYA' then '8216'
                WHEN`City` = 'KABUPATEN SARMI' then '8217'
                WHEN`City` = 'KABUPATEN KEEROM' then '8218'
                WHEN`City` = 'KABUPATEN PEGUNUNGAN BINTANG' then '8221'
                WHEN`City` = 'KABUPATEN YAHUKIMO' then '8222'
                WHEN`City` = 'KABUPATEN TOLIKARA' then '8223'
                WHEN`City` = 'KABUPATEN WAROPEN' then '8224'
                WHEN`City` = 'KABUPATEN BOVEN DIGOEL' then '8226'
                WHEN`City` = 'KABUPATEN MAPPI' then '8227'
                WHEN`City` = 'KABUPATEN ASMAT' then '8228'
                WHEN`City` = 'KABUPATEN SUPIORI' then '8231'
                WHEN`City` = 'KABUPATEN MAMBERAMO RAYA' then '8232'
                WHEN`City` = 'KABUPATEN DOGIYAI' then '8233'
                WHEN`City` = 'KABUPATEN LANNY JAYA' then '8234'
                WHEN`City` = 'KABUPATEN MAMBERAMO TENGAH' then '8235'
                WHEN`City` = 'KABUPATEN NDUGA' then '8236'
                WHEN`City` = 'KABUPATEN YALIMO' then '8237'
                WHEN`City` = 'KABUPATEN PUNCAK' then '8238'
                WHEN`City` = 'KABUPATEN DEIYA' then '8240'
                WHEN`City` = 'KOTA JAYAPURA' then '8291'
                WHEN`City` = 'KABUPATEN INTAN JAYA' then '8239'
                WHEN`City` = 'PROVINSI MALUKU UTARA' then '8300'
                WHEN`City` = 'KABUPATEN HALMAHERA TENGAH' then '8302'
                WHEN`City` = 'KABUPATEN HALMAHERA UTARA' then '8303'
                WHEN`City` = 'KABUPATEN HALMAHERA TIMUR' then '8304'
                WHEN`City` = 'KABUPATEN HALMAHERA BARAT' then '8305'
                WHEN`City` = 'KABUPATEN HALMAHERA SELATAN' then '8306'
                WHEN`City` = 'KABUPATEN KEPULAUAN SULA' then '8307'
                WHEN`City` = 'KABUPATEN PULAU MOROTAI' then '8308'
                WHEN`City` = 'KOTA TERNATE' then '8390'
                WHEN`City` = 'KOTA TIDORE KEPULAUAN' then '8391'
                WHEN`City` = 'KABUPATEN PULAU TALIABU' then '8309'
                WHEN`City` = 'PROVINSI PAPUA BARAT' then '8400'
                WHEN`City` = 'KABUPATEN SORONG' then '8401'
                WHEN`City` = 'KABUPATEN FAK-FAK' then '8402'
                WHEN`City` = 'KABUPATEN MANOKWARI' then '8403'
                WHEN`City` = 'KABUPATEN SORONG SELATAN' then '8404'
                WHEN`City` = 'KABUPATEN RAJA AMPAT' then '8405'
                WHEN`City` = 'KABUPATEN KAIMANA' then '8406'
                WHEN`City` = 'KABUPATEN TELUK BINTUNI' then '8407'
                WHEN`City` = 'KABUPATEN TELUK WONDAMA' then '8408'
                WHEN`City` = 'KABUPATEN TEMBRAUW' then '8409'
                WHEN`City` = 'KABUPATEN MAYBRAT' then '8410'
                WHEN`City` = 'KOTA SORONG' then '8491'
                WHEN`City` = 'KABUPATEN PEGUNUNGAN ARFAK' then '8411'
                WHEN`City` = 'KABUPATEN MANOKWARI SELATAN' then '8412'
                WHEN`City` = 'KABUPATEN BANYU ASIN' then '3607'
                WHEN`City` = 'JAKARTA PUSAT' then '0391'
                WHEN`City` = 'DI LUAR INDONESIA' then '9999'
                ELSE '0000'
            END AS `Kode_Kabupaten_atau_Kota_Lokasi_Proyek_atau_Penggunaan_Kredit_atau_Pembiayaan`,
            `Nilai_Proyek`,
            `Kode_Valuta`,
            `Suku_Bunga_atau_Imbalan`,
            `Jenis_Suku_Bunga_atau_Imbalan`,
            `Kredit_atau_Pembiayaan_Program_Pemerintah`,
            `Plafon_Awal`,
            `Plafon`,
            `Realisasi_atau_Pencairan_Bulan_Berjalan`,
            CASE
                WHEN Kode_Kondisi = '02' 
                    OR Kode_Kualitas_Kredit_atau_Pembiayaan = '1'
                    THEN 0
                ELSE
                    `Denda`
            END AS `Denda`,
            `Baki_Debet`,
            `Nilai_dalam_Mata_Uang_Asal`,
            `Kode_Kualitas_Kredit_atau_Pembiayaan`,
            `Tanggal_Macet`,
            `Kode_Sebab_Macet`,
            CASE
                WHEN Kode_Kondisi = '02' 
                    OR Kode_Kualitas_Kredit_atau_Pembiayaan = '1'
                    THEN 0
                ELSE
                    `Tunggakan_Bunga_atau_Imbalan`
            END AS `Tunggakan_Bunga_atau_Imbalan`,
            `Jumlah_Hari_Tunggakan`,
            CASE
            WHEN Baki_Debet = 0 THEN 0
            WHEN `Frekuensi_Tunggakan` < 0 OR (`Frekuensi_Tunggakan` = 0 AND Jumlah_Hari_Tunggakan > 0)
                    OR Frekuensi_Tunggakan > approvedLoanTenureInMonth
                THEN approvedLoanTenureInMonth
            ELSE `Frekuensi_Tunggakan`
            END Frekuensi_Tunggakan,
            `Frekuensi_Restrukturisasi`,
            `Tanggal_Restrukturisasi_Awal`,
            `Tanggal_Restrukturisasi_Akhir`,
            `Kode_Cara_Restrukturisasi`,
            `Kode_Kondisi`,
            FORMAT_DATE('%Y%m%d',date(`Tanggal_Kondisi`)) `Tanggal_Kondisi`,
            `Keterangan`,
            `Kode_Kantor_Cabang`,
            `Operasi_Data`,
            `Status_delete`,
            `Create_Date`,
            `Update_Date`,
            Approved_Date,
            userId,
            orderId,
            dayPastDue,
            approvedLoanAmount,
            approvedLoanTenureInMonth,
            maxdayPastDue,
            `lateFee`,
            balanceWithInterest,
            balance,
            dataDate,
            ceiling(approvedLoanAmount/approvedLoanTenureInMonth) as jumlah_cicilan,
            current_payment_periode_month,
            count_payment,
            paidoffDate,
            status
            from `summary`
            WHERE row_number = 1
            order by orderId, Approved_Date
            )
            """.format(date=year_date, filter_q=filter_q)

        start_query = time.time()
        query_job = client.query(sql)
        df = query_job.to_dataframe()
        end_query = time.time()
        print("[get_F01_data] Query from BigQuery takes {}".format(end_query-start_query))
        self.F01new = df
        return self.F01new

    def get_D01_data(self, year_date, filter=True):
        client = bigquery.Client(project="athena-179008")

        filter_q=''

        if filter:
            filter_q = """AND (c.status = 'ACTIVE'
                        OR (c.status IN ('FINISHED', 'FINISHED_RESTRUCTURED') AND date(c.updatedAt) BETWEEN date_trunc(DATE_SUB(DATE({date}), INTERVAL 33 DAY), MONTH) AND date_trunc(DATE({date}), MONTH))
                        OR (c.status IN ('REJECTED', 'CANCELLED') AND date(c.updatedAt) >= date_trunc(DATE_SUB(DATE({date}), INTERVAL 33 DAY), MONTH)))
                        AND date(c.`approvedDate`) < date_trunc(DATE({date}), MONTH)""".format(date=year_date)

        sql = """
                WITH npwp AS (
                    SELECT DISTINCT N.userId,
                                    N.npwp
                    FROM `vayu.indodana_athena_applications` A
                    INNER JOIN `vayu.indodana_athena_contracts` C ON A.orderId = C.applicationOrderId
                    INNER JOIN `vayu.indodana_athena_npwp_validations` N ON A.userId = N.userId
                    WHERE N.isValid = TRUE 
                )
                , user_virtual_account AS (
                    SELECT userId, virtualAccountNumber
                    FROM `vayu.indodana_athena_virtual_accounts`
                    WHERE virtualAccountBank = 'BCA'
                )
                , user_virtual_account_subs AS (
                    SELECT userId, virtualAccountNumber
                    FROM (
                        SELECT vc.userId, virtualAccountBank, virtualAccountNumber, active, vc.createdAt, approvedDate
                                ,   row_number() over (PARTITION by vc.userId ORDER by vc.createdAt asc) as row_number
                        FROM `vayu.indodana_athena_virtual_accounts` vc
                        JOIN vayu.indodana_athena_applications a ON vc.userId = a.userId
                        JOIN vayu.indodana_athena_contracts c ON a.orderId = c.applicationOrderId
                        WHERE virtualAccountBank != 'BCA'
                                AND DATE(vc.createdAt) = DATE(approvedDate)
                        ORDER BY userId, vc.createdAt
                    )
                    WHERE row_number = 1
                )
                , test_apps AS (
                    SELECT orderId
                    FROM `vayu.indodana_athena_applications`
                    WHERE (applicantPersonalEmail LIKE '%@cermati%'
                            OR applicantPersonalEmail LIKE '%@indodana%'
                            OR applicantPersonalEmail = 'akoesnan2@gmail.com')
                            OR orderId LIKE '%LOP%'
                            OR orderId LIKE 'LBH%'
                )
                , summary AS (
                    SELECT '0301' AS Kode_Jenis_Pelapor
                        ,'900013' AS Kode_Pelapor
                        ,CAST(FORMAT_DATE('%Y%m', DATE({date})) AS STRING) AS Tahun_Bulan_Data
                        ,'' AS Nomor_CIF_Lama_Debitur
                        ,replace(coalesce(CAST(uv.virtualAccountNumber AS STRING), CAST(uvs.virtualAccountNumber AS STRING), 'NULL'),'+','') AS Nomor_CIF_Debitur
                        ,1 AS Jenis_Identitas
                        ,CAST(a.applicantIdNumber AS STRING) AS Nomor_Identitas
                        ,coalesce(kv.name, a.applicantName) AS Nama_Sesuai_Identitas
                        ,a.applicantName AS Nama_Lengkap
                        ,CASE
                            WHEN a.applicantLastEducationLevel = 'S1' THEN '04'
                            WHEN a.applicantLastEducationLevel = 'S2' THEN '05'
                            WHEN a.applicantLastEducationLevel = 'S3' THEN '06'
                            WHEN a.applicantLastEducationLevel = 'Akademi/D3' THEN '03'
                            WHEN a.applicantLastEducationLevel IN ('SD', 'SMP', 'SMA') THEN '00'
                            WHEN a.applicantLastEducationLevel IS NULL THEN '00'
                            ELSE '99'
                        END AS Kode_Status_Pendidikan_atau_Gelar_Debitur
                        ,CASE
                            WHEN a.applicantGender = 'MALE' THEN 'L'
                            ELSE 'P'
                        END AS Jenis_Kelamin
                        ,coalesce(a.applicantPlaceOfBirthCity, kv.placeOfBirthCity) AS Tempat_Lahir
                        ,coalesce(FORMAT_DATE('%Y%m%d', DATE(a.applicantDateOfBirth)), FORMAT_DATE('%Y%m%d', DATE(kv.birthDate))) AS Tanggal_Lahir
                        ,a.applicantMothersMaidenName AS Nama_Gadis_Ibu_Kandung
                        ,CASE WHEN CAST(n.npwp AS STRING) IS NOT NULL AND CAST(n.npwp AS STRING) <> '' and CHAR_LENGTH(CAST(n.npwp AS STRING)) = 15 THEN CAST(n.npwp AS STRING) else NULL 
                        END AS NPWP
                        ,CASE
                            WHEN a.addressIsCurrentAddress = 'Ya' THEN replace(CAST(a.applicantAddress AS STRING), ',', '')
                            WHEN a.applicantCurrentAddress is not null then replace(CAST(a.applicantCurrentAddress AS STRING), ',', '')
                            WHEN a.currentCompanyAddress is not null then replace(CAST(a.currentCompanyAddress AS STRING), ',', '')
                        ELSE replace(CAST(kv.address AS STRING), ',', '')
                        END AS Alamat
                        ,coalesce(coalesce(coalesce(a.applicantCurrentResidenceVillage,a.applicantResidenceVillage), a.currentCompanyAddressVillage), kv.village) AS Kelurahan
                        ,coalesce(coalesce(coalesce(a.applicantCurrentResidenceDistrict,a.applicantResidenceDistrict), a.currentCompanyAddressDistrict), kv.district) AS Kecamatan
                        ,CASE
                            WHEN a.addressIsCurrentAddress = 'Ya' THEN a.applicantResidenceCity
                            ELSE a.applicantCurrentResidenceCity
                        END AS City
                        ,CASE
                            WHEN a.addressIsCurrentAddress = 'Ya' THEN a.applicantResidencePostalCode
                            ELSE a.applicantCurrentResidencePostalCode
                        END AS Kode_Pos
                        ,replace(CAST(a.applicantMobilePhoneNumber1 AS STRING),'+62','62') AS Nomor_Telepon
                        ,replace(CAST(a.applicantMobilePhoneNumber1 AS STRING),'+62','62') AS Nomor_Telepon_Seluler
                        ,a.applicantPersonalEmail AS Alamat_email
                        ,'ID' AS Kode_Negara_Domisili
                        ,'099' AS Kode_Pekerjaan
                        ,replace(a.currentCompanyName, '|', '') AS Tempat_Bekerja
                        ,CASE
                            WHEN upper(a.currentCompanyIndustry) = 'ASURANSI/FOREX' THEN '660000'
                            WHEN upper(a.currentCompanyIndustry) = 'HUKUM DAN PERPAJAKAN' THEN '741000'
                            WHEN upper(a.currentCompanyIndustry) = 'INDUSTRI/PABRIK' THEN '369000'
                            WHEN upper(a.currentCompanyIndustry) = 'JASA/PELAYANAN' THEN '930000'
                            WHEN upper(a.currentCompanyIndustry) = 'KESEHATAN/KLINIK' THEN '851001'
                            WHEN upper(a.currentCompanyIndustry) = 'KEUANGAN/BANK' THEN '651000'
                            WHEN upper(a.currentCompanyIndustry) = 'KONSULTAN' THEN '741000'
                            WHEN upper(a.currentCompanyIndustry) = 'KONTRAKTOR/PROPERTI' THEN '701009'
                            WHEN upper(a.currentCompanyIndustry) = 'PARIWISATA' THEN '703000'

                            WHEN upper(a.currentCompanyIndustry) = 'PEMERINTAHAN' THEN '751000'
                            WHEN upper(a.currentCompanyIndustry) = 'PERDAGANGAN' THEN '009000'
                            WHEN upper(a.currentCompanyIndustry) = 'PERKEBUNAN' THEN '011190'
                            WHEN upper(a.currentCompanyIndustry) = 'PERTAMBANGAN' THEN '142900'
                            WHEN upper(a.currentCompanyIndustry) = 'RESTORAN/BAR/CAFE' THEN '552009'
                            WHEN upper(a.currentCompanyIndustry) = 'RETAIL' THEN '523900'
                            WHEN upper(a.currentCompanyIndustry) = 'SPESIALIS' THEN '009000'
                            WHEN upper(a.currentCompanyIndustry) = 'TELEKOMUNIKASI' THEN '643000'
                            WHEN upper(a.currentCompanyIndustry) = 'TRANSPORTASI' THEN '359900'
                            ELSE '000001'
                        END AS Kode_Bidang_Usaha_Tempat_Bekerja
                        ,replace(replace(CAST(a.currentCompanyAddress AS STRING), ',', ''), '|', '') AS Alamat_Tempat_Bekerja
                        ,'9000' AS Kode_Golongan_Debitur
                        ,CASE
                            WHEN a.applicantMarriageStatus = 'MARRIED' THEN 1
                            WHEN a.applicantMarriageStatus = 'SINGLE' THEN 2
                            WHEN a.applicantMarriageStatus = 'DIVORCED' THEN 3
                            ELSE NULL
                        END AS Status_Perkawinan_Debitur
                        ,'000' AS Kode_Kantor_Cabang
                        ,'C' AS Operasi_Data
                        ,'T' AS Status_delete
                        ,c.createdAt AS Create_Date
                        ,c.updatedAt AS Update_Date
                        ,date(c.`approvedDate`) AS Approved_Date
                        ,a.userId
                        ,a.orderId
                        ,c.status
                        ,c.paidoffDate
                        ,row_number() over (PARTITION by a.userId ORDER by replace(coalesce(CAST(uv.virtualAccountNumber AS STRING), CAST(uvs.virtualAccountNumber AS STRING), 'NULL'),'+',''), c.approvedDate desc) as row_number --used to eliminate double user_id
                    FROM `vayu.indodana_athena_applications` a
                    INNER JOIN `vayu_data_mart.indodana_athena_contracts` c ON a.orderId = c.applicationOrderId AND c.dataDate = date_trunc(DATE({date}), MONTH)
                    LEFT join `vayu.indodana_athena_ktp_validations` kv on a.applicantIdNumber = kv.nik
                    LEFT JOIN `vayu.indodana_athena_npwp_validations` n ON a.userId = n.userId
                    LEFT JOIN user_virtual_account uv on a.userId  = uv.userId
                    LEFT JOIN user_virtual_account_subs uvs on a.userId  = uvs.userId
                    WHERE  a.orderId NOT IN (SELECT orderId
                                            FROM test_apps)
                    {filter_q}
                        
                )
                SELECT Kode_Jenis_Pelapor,
                    Kode_Pelapor,
                    Tahun_Bulan_Data,
                    Nomor_CIF_Lama_Debitur,
                    Nomor_CIF_Debitur,
                    Jenis_Identitas,
                    Nomor_Identitas,
                    UPPER(replace(regexp_replace(Nama_Sesuai_Identitas, r"([\\n\\r]+)", ' '),'|','')) AS Nama_Sesuai_Identitas,
                    UPPER(replace(regexp_replace(Nama_Lengkap, r"([\\n\\r]+)", ' '),'|','')) AS Nama_Lengkap,
                    Kode_Status_Pendidikan_atau_Gelar_Debitur,
                    Jenis_Kelamin,
                    Tempat_Lahir,
                    Tanggal_Lahir,
                    UPPER(replace(regexp_replace(Nama_Gadis_Ibu_Kandung, r"([\\n\\r]+)", ' '),'|','')) AS Nama_Gadis_Ibu_Kandung,
                    NPWP,
                    LPAD(regexp_replace(regexp_replace(regexp_replace(Alamat, r"([\\n\\r]+)", ' '), r"([\W_] -.,+)", ' '), r" +", ' '), 300) Alamat,
                    Kelurahan,
                    Kecamatan,
                    CASE
                        WHEN City = 'PROVINSI JAWA BARAT' then '0100'
                        WHEN City = 'KABUPATEN BEKASI' then '0102'
                        WHEN City = 'KABUPATEN PURWAKARTA' then '0103'
                        WHEN City = 'KABUPATEN KARAWANG' then '0106'
                        WHEN City = 'KABUPATEN BOGOR' then '0108'
                        WHEN City = 'BOGOR' then '0108'
                        WHEN City = 'KABUPATEN SUKABUMI' then '0109'
                        WHEN City = 'KABUPATEN CIANJUR' then '0110'
                        WHEN City = 'KABUPATEN BANDUNG' then '0111'
                        WHEN City = 'KABUPATEN SUMEDANG' then '0112'
                        WHEN City = 'KABUPATEN TASIKMALAYA' then '0113'
                        WHEN City = 'KABUPATEN GARUT' then '0114'
                        WHEN City = 'KABUPATEN CIAMIS' then '0115'
                        WHEN City = 'KABUPATEN CIREBON' then '0116'
                        WHEN City = 'KABUPATEN KUNINGAN' then '0117'
                        WHEN City = 'KABUPATEN INDRAMAYU' then '0118'
                        WHEN City = 'KABUPATEN MAJALENGKA' then '0119'
                        WHEN City = 'KABUPATEN SUBANG' then '0121'
                        WHEN City = 'KABUPATEN BANDUNG BARAT' then '0122'
                        WHEN City = 'KOTA BANJAR' then '0180'
                        WHEN City = 'KOTA BANDUNG' then '0191'
                        WHEN City = 'KOTA BOGOR' then '0192'
                        WHEN City = 'KOTA SUKABUMI' then '0193'
                        WHEN City = 'KOTA CIREBON' then '0194'
                        WHEN City = 'KOTA TASIKMALAYA' then '0195'
                        WHEN City = 'KOTA CIMAHI' then '0196'
                        WHEN City = 'KOTA DEPOK' then '0197'
                        WHEN City = 'KOTA BEKASI' then '0198'
                        WHEN City = 'KABUPATEN PANGANDARAN' then '0123'
                        WHEN City = 'PROVINSI BANTEN' then '0200'
                        WHEN City = 'KABUPATEN LEBAK' then '0201'
                        WHEN City = 'KABUPATEN PANDEGLANG' then '0202'
                        WHEN City = 'KABUPATEN SERANG' then '0203'
                        WHEN City = 'KABUPATEN TANGERANG' then '0204'
                        WHEN City = 'KOTA CILEGON' then '0291'
                        WHEN City = 'KOTA TANGERANG' then '0292'
                        WHEN City = 'KOTA SERANG' then '0293'
                        WHEN City = 'KOTA TANGERANG SELATAN' then '0294'
                        WHEN City = 'KOTA BANTEN' then '0294'
                        WHEN City = 'PROVINSI DKI JAYA' then '0300'
                        WHEN City = 'KOTA JAKARTA PUSAT' then '0391'
                        WHEN City = 'KOTA JAKARTA UTARA' then '0392'
                        WHEN City = 'KOTA JAKARTA BARAT' then '0393'
                        WHEN City = 'KOTA JAKARTA SELATAN' then '0394'
                        WHEN City = 'KOTA ADM. JAKARTA SELATAN' then '0394'
                        WHEN City = 'KOTA JAKARTA TIMUR' then '0395'
                        WHEN City = 'KEPULAUAN SERIBU' then '0396'
                        WHEN City = 'KABUPATEN KEPULAUAN SERIBU' then '0396'
                        WHEN City = 'DAERAH ISTIMEWA YOGYAKARTA' then '0500'
                        WHEN City = 'KABUPATEN BANTUL' then '0501'
                        WHEN City = 'KABUPATEN SLEMAN' then '0502'
                        WHEN City = 'KABUPATEN GUNUNG KIDUL' then '0503'
                        WHEN City = 'KABUPATEN KULON PROGO' then '0504'
                        WHEN City = 'KOTA YOGYAKARTA' then '0591'
                        WHEN City = 'KABUPATEN PEKALONGAN' then '0905'
                        WHEN City = 'KABUPATEN TEGAL' then '0906'
                        WHEN City = 'KABUPATEN BREBES' then '0907'
                        WHEN City = 'KABUPATEN PATI' then '0908'
                        WHEN City = 'KABUPATEN KUDUS' then '0909'
                        WHEN City = 'KABUPATEN PEMALANG' then '0910'
                        WHEN City = 'KABUPATEN JEPARA' then '0911'
                        WHEN City = 'KABUPATEN REMBANG' then '0912'
                        WHEN City = 'KABUPATEN BLORA' then '0913'
                        WHEN City = 'KABUPATEN BANYUMAS' then '0914'
                        WHEN City = 'KABUPATEN CILACAP' then '0915'
                        WHEN City = 'KABUPATEN PURBALINGGA' then '0916'
                        WHEN City = 'KABUPATEN BANJARNEGARA' then '0917'
                        WHEN City = 'KABUPATEN MAGELANG' then '0918'
                        WHEN City = 'KABUPATEN TEMANGGUNG' then '0919'
                        WHEN City = 'KABUPATEN WONOSOBO' then '0920'
                        WHEN City = 'KABUPATEN PURWOREJO' then '0921'
                        WHEN City = 'KABUPATEN KEBUMEN' then '0922'
                        WHEN City = 'KABUPATEN KLATEN' then '0923'
                        WHEN City = 'KABUPATEN BOYOLALI' then '0924'
                        WHEN City = 'KABUPATEN SRAGEN' then '0925'
                        WHEN City = 'KABUPATEN SUKOHARJO' then '0926'
                        WHEN City = 'KABUPATEN KARANGANYAR' then '0927'
                        WHEN City = 'KABUPATEN WONOGIRI' then '0928'
                        WHEN City = 'KABUPATEN BATANG' then '0929'
                        WHEN City = 'KOTA SEMARANG' then '0991'
                        WHEN City = 'KOTA SALATIGA' then '0992'
                        WHEN City = 'KOTA PEKALONGAN' then '0993'
                        WHEN City = 'KOTA TEGAL' then '0994'
                        WHEN City = 'KOTA MAGELANG' then '0995'
                        WHEN City = 'KOTA SURAKARTA' then '0996'
                        WHEN City = 'KOTA SOLO' then '0996'
                        WHEN City = 'KABUPATEN GROBOGAN' then '0904'
                        WHEN City = 'PROVINSI JAWA TENGAH' then '0900'
                        WHEN City = 'KABUPATEN SEMARANG' then '0901'
                        WHEN City = 'KABUPATEN KENDAL' then '0902'
                        WHEN City = 'KABUPATEN DEMAK' then '0903'
                        WHEN City = 'PROVINSI JAWA TIMUR' then '1200'
                        WHEN City = 'KABUPATEN GRESIK' then '1201'
                        WHEN City = 'KABUPATEN SIDOARJO' then '1202'
                        WHEN City = 'KABUPATEN MOJOKERTO' then '1203'
                        WHEN City = 'KABUPATEN JOMBANG' then '1204'
                        WHEN City = 'KABUPATEN SAMPANG' then '1205'
                        WHEN City = 'KABUPATEN PAMEKASAN' then '1206'
                        WHEN City = 'KABUPATEN SUMENEP' then '1207'
                        WHEN City = 'KABUPATEN BANGKALAN' then '1208'
                        WHEN City = 'KABUPATEN BONDOWOSO' then '1209'
                        WHEN City = 'KABUPATEN BANYUWANGI' then '1211'
                        WHEN City = 'KABUPATEN JEMBER' then '1212'
                        WHEN City = 'KABUPATEN MALANG' then '1213'
                        WHEN City = 'KABUPATEN PASURUAN' then '1214'
                        WHEN City = 'KABUPATEN PROBOLINGGO' then '1215'
                        WHEN City = 'KABUPATEN LUMAJANG' then '1216'
                        WHEN City = 'KABUPATEN KEDIRI' then '1217'
                        WHEN City = 'KABUPATEN NGANJUK' then '1218'
                        WHEN City = 'KABUPATEN TULUNGAGUNG' then '1219'
                        WHEN City = 'KABUPATEN TRENGGALEK' then '1220'
                        WHEN City = 'KABUPATEN BLITAR' then '1221'
                        WHEN City = 'KABUPATEN MADIUN' then '1222'
                        WHEN City = 'KABUPATEN NGAWI' then '1223'
                        WHEN City = 'KABUPATEN MAGETAN' then '1224'
                        WHEN City = 'KABUPATEN PONOROGO' then '1225'
                        WHEN City = 'KABUPATEN PACITAN' then '1226'
                        WHEN City = 'KABUPATEN BOJONEGORO' then '1227'
                        WHEN City = 'KABUPATEN TUBAN' then '1228'
                        WHEN City = 'KABUPATEN LAMONGAN' then '1229'
                        WHEN City = 'KABUPATEN SITUBONDO' then '1230'
                        WHEN City = 'KOTA BATU' then '1271'
                        WHEN City = 'KOTA SURABAYA' then '1291'
                        WHEN City = 'KOTA MOJOKERTO' then '1292'
                        WHEN City = 'KOTA MALANG' then '1293'
                        WHEN City = 'KOTA PASURUAN' then '1294'
                        WHEN City = 'KOTA PROBOLINGGO' then '1295'
                        WHEN City = 'KOTA BLITAR' then '1296'
                        WHEN City = 'KOTA KEDIRI' then '1297'
                        WHEN City = 'KOTA MADIUN' then '1298'
                        WHEN City = 'PROVINSI BENGKULU' then '2300'
                        WHEN City = 'KABUPATEN BENGKULU SELATAN' then '2301'
                        WHEN City = 'KABUPATEN BENGKULU UTARA' then '2302'
                        WHEN City = 'KABUPATEN REJANG LEBONG' then '2303'
                        WHEN City = 'KABUPATEN LEBONG' then '2304'
                        WHEN City = 'KABUPATEN KEPAHIANG' then '2305'
                        WHEN City = 'KABUPATEN MUKOMUKO' then '2306'
                        WHEN City = 'KABUPATEN SELUMA' then '2307'
                        WHEN City = 'KABUPATEN KAUR' then '2308'
                        WHEN City = 'KABUPATEN BENGKULU TENGAH' then '2309'
                        WHEN City = 'KOTA BENGKULU' then '2391'
                        WHEN City = 'PROVINSI JAMBI' then '3100'
                        WHEN City = 'KABUPATEN BATANGHARI' then '3101'
                        WHEN City = 'KABUPATEN SAROLANGUN' then '3104'
                        WHEN City = 'KABUPATEN KERINCI' then '3105'
                        WHEN City = 'KABUPATEN MUARO JAMBI' then '3106'
                        WHEN City = 'KABUPATEN TANJUNG JABUNG BARAT' then '3107'
                        WHEN City = 'KABUPATEN TANJUNG JABUNG TIMUR' then '3108'
                        WHEN City = 'KABUPATEN TEBO' then '3109'
                        WHEN City = 'KABUPATEN MERANGIN' then '3111'
                        WHEN City = 'KABUPATEN BUNGO' then '3112'
                        WHEN City = 'KOTA JAMBI' then '3191'
                        WHEN City = 'KOTA SUNGAI PENUH' then '3192'
                        WHEN City = 'PROVINSI NAD' then '3200'
                        WHEN City = 'KABUPATEN ACEH BESAR' then '3201'
                        WHEN City = 'KABUPATEN PIDIE' then '3202'
                        WHEN City = 'KABUPATEN ACEH UTARA' then '3203'
                        WHEN City = 'KABUPATEN ACEH TIMUR' then '3204'
                        WHEN City = 'KABUPATEN ACEH SELATAN' then '3205'
                        WHEN City = 'KABUPATEN ACEH BARAT' then '3206'
                        WHEN City = 'KABUPATEN ACEH TENGAH' then '3207'
                        WHEN City = 'KABUPATEN ACEH TENGGARA' then '3208'
                        WHEN City = 'KABUPATEN ACEH SINGKIL' then '3209'
                        WHEN City = 'KABUPATEN ACEH JEUMPA /BIREUEN' then '3210'
                        WHEN City = 'KABUPATEN ACEH TAMIANG' then '3211'
                        WHEN City = 'KABUPATEN GAYO LUWES' then '3212'
                        WHEN City = 'KABUPATEN ACEH BARAT DAYA' then '3213'
                        WHEN City = 'KABUPATEN ACEH JAYA' then '3214'
                        WHEN City = 'KABUPATEN NAGAN RAYA' then '3215'
                        WHEN City = 'KABUPATEN SIMEULEU' then '3216'
                        WHEN City = 'KABUPATEN BENER MERIAH' then '3217'
                        WHEN City = 'KABUPATEN PIDIE JAYA' then '3218'
                        WHEN City = 'KABUPATEN SUBULUSSALAM' then '3219'
                        WHEN City = 'KOTA BANDA ACEH' then '3291'
                        WHEN City = 'KOTA SABANG' then '3292'
                        WHEN City = 'KOTA LHOKSEUMAWE' then '3293'
                        WHEN City = 'KOTA LANGSA' then '3294'
                        WHEN City = 'PROVINSI SUMATERA UTARA' then '3300'
                        WHEN City = 'KABUPATEN DELI SERDANG' then '3301'
                        WHEN City = 'KABUPATEN LANGKAT' then '3302'
                        WHEN City = 'KABUPATEN KARO' then '3303'
                        WHEN City = 'KABUPATEN SIMALUNGUN' then '3304'
                        WHEN City = 'KABUPATEN LABUHAN BATU' then '3305'
                        WHEN City = 'KABUPATEN ASAHAN' then '3306'
                        WHEN City = 'KABUPATEN DAIRI' then '3307'
                        WHEN City = 'KABUPATEN TAPANULI UTARA' then '3308'
                        WHEN City = 'KABUPATEN TAPANULI TENGAH' then '3309'
                        WHEN City = 'KABUPATEN TAPANULI SELATAN' then '3310'
                        WHEN City = 'KABUPATEN NIAS' then '3311'
                        WHEN City = 'KABUPATEN TOBA SAMOSIR' then '3313'
                        WHEN City = 'KABUPATEN MANDAILING NATAL' then '3314'
                        WHEN City = 'KABUPATEN NIAS SELATAN' then '3315'
                        WHEN City = 'KABUPATEN HUMBANG HASUNDUTAN' then '3316'
                        WHEN City = 'KABUPATEN PAKPAK BHARAT' then '3317'
                        WHEN City = 'KABUPATEN SAMOSIR' then '3318'
                        WHEN City = 'KABUPATEN SERDANG BEDAGAI' then '3319'
                        WHEN City = 'KABUPATEN BATU BARA' then '3321'
                        WHEN City = 'KABUPATEN PADANG LAWAS' then '3322'
                        WHEN City = 'KABUPATEN PADANG LAWAS UTARA' then '3323'
                        WHEN City = 'KABUPATEN LABUANBATU SELATAN' then '3324'
                        WHEN City = 'KABUPATEN LABUANBATU UTARA' then '3325'
                        WHEN City = 'KABUPATEN NIAS BARAT' then '3326'
                        WHEN City = 'KABUPATEN NIAS UTARA' then '3327'
                        WHEN City = 'KOTA TEBING TINGGI' then '3391'
                        WHEN City = 'KOTA BINJAI' then '3392'
                        WHEN City = 'KOTA PEMATANG SIANTAR' then '3393'
                        WHEN City = 'KOTA TANJUNG BALAI' then '3394'
                        WHEN City = 'KOTA SIBOLGA' then '3395'
                        WHEN City = 'KOTA MEDAN' then '3396'
                        WHEN City = 'KOTA GUNUNG SITOLI' then '3397'
                        WHEN City = 'KOTA PADANG SIDEMPUAN' then '3399'
                        WHEN City = 'PROVINSI SUMATERA BARAT' then '3400'
                        WHEN City = 'KABUPATEN AGAM' then '3401'
                        WHEN City = 'KABUPATEN PASAMAN' then '3402'
                        WHEN City = 'KABUPATEN LIMAPULUH KOTA' then '3403'
                        WHEN City = 'KABUPATEN SOLOK SELATAN' then '3404'
                        WHEN City = 'KABUPATEN PADANG PARIAMAN' then '3405'
                        WHEN City = 'KABUPATEN PESISIR SELATAN' then '3406'
                        WHEN City = 'KABUPATEN TANAH DATAR' then '3407'
                        WHEN City = 'KABUPATEN SAWAH LUNTO/SIJUNJUNG' then '3408'
                        WHEN City = 'KABUPATEN KEPULAUAN MENTAWAI' then '3409'
                        WHEN City = 'KABUPATEN PASAMAN BARAT' then '3410'
                        WHEN City = 'KABUPATEN DHARMASRAYA' then '3411'
                        WHEN City = 'KABUPATEN SOLOK' then '3412'
                        WHEN City = 'KOTA BUKITTINGGI' then '3491'
                        WHEN City = 'KOTA PADANG' then '3492'
                        WHEN City = 'KOTA SAWAHLUNTO' then '3493'
                        WHEN City = 'KOTA PADANG PANJANG' then '3494'
                        WHEN City = 'KOTA SOLOK' then '3495'
                        WHEN City = 'KOTA PAYAKUMBUH' then '3496'
                        WHEN City = 'KOTA PARIAMAN' then '3497'
                        WHEN City = 'PROVINSI RIAU' then '3500'
                        WHEN City = 'KABUPATEN KAMPAR' then '3501'
                        WHEN City = 'KABUPATEN BENGKALIS' then '3502'
                        WHEN City = 'KABUPATEN INDRAGIRI HULU' then '3504'
                        WHEN City = 'KABUPATEN INDRAGIRI HILIR' then '3505'
                        WHEN City = 'KABUPATEN ROKAN HULU' then '3508'
                        WHEN City = 'KABUPATEN ROKAN HILIR' then '3509'
                        WHEN City = 'KABUPATEN PELALAWAN' then '3510'
                        WHEN City = 'KABUPATEN SIAK' then '3511'
                        WHEN City = 'KABUPATEN KUANTAN SINGINGI' then '3512'
                        WHEN City = 'KABUPATEN KEPULAUAN MERANTI' then '3513'
                        WHEN City = 'KOTA PEKANBARU' then '3591'
                        WHEN City = 'KOTA DUMAI' then '3592'
                        WHEN City = 'PROVINSI SUMATERA SELATAN' then '3600'
                        WHEN City = 'KABUPATEN MUSI BANYUASIN' then '3606'
                        WHEN City = 'KABUPATEN OGAN KOMERING ULU' then '3607'
                        WHEN City = 'KABUPATEN MUARA ENIM' then '3608'
                        WHEN City = 'KABUPATEN LAHAT' then '3609'
                        WHEN City = 'KABUPATEN MUSI RAWAS' then '3610'
                        WHEN City = 'KABUPATEN OGAN KOMERING ILIR' then '3611'
                        WHEN City = 'KABUPATEN BANYUASIN' then '3613'
                        WHEN City = 'KABUPATEN OGAN KOMERING ULU SELATAN' then '3614'
                        WHEN City = 'KABUPATEN OGAN KOMERING ULU TIMUR' then '3615'
                        WHEN City = 'KABUPATEN OGAN ILIR' then '3616'
                        WHEN City = 'KABUPATEN PENUKAL ABAB LEMATANG ILIR' then '3619'
                        WHEN City = 'KOTA PALEMBANG' then '3691'
                        WHEN City = 'KOTA LUBUKLINGGAU' then '3693'
                        WHEN City = 'KOTA PRABUMULIH' then '3694'
                        WHEN City = 'KOTA PAGAR ALAM' then '3697'
                        WHEN City = 'KABUPATEN MUSI RAWAS UTARA' then '3618'
                        WHEN City = 'KABUPATEN EMPAT LAWANG' then '3617'
                        WHEN City = 'PROVINSI KEPULAUAN BANGKA BELITUNG' then '3700'
                        WHEN City = 'KABUPATEN BANGKA' then '3701'
                        WHEN City = 'KABUPATEN BELITUNG' then '3702'
                        WHEN City = 'KABUPATEN BANGKA BARAT' then '3703'
                        WHEN City = 'KABUPATEN BANGKA SELATAN' then '3704'
                        WHEN City = 'KABUPATEN BANGKA TENGAH' then '3705'
                        WHEN City = 'KABUPATEN BELITUNG TIMUR' then '3706'
                        WHEN City = 'KABUPATEN BANGKA BELITUNG(LBU 2016)/KOTA PANGKAL PINANG' then '3707'
                        WHEN City = 'KOTA PANGKAL PINANG' then '3791'
                        WHEN City = 'PROVINSI KEPULAUAN RIAU' then '3800'
                        WHEN City = 'KABUPATEN KARIMUN' then '3801'
                        WHEN City = 'KABUPATEN LINGGA' then '3802'
                        WHEN City = 'KABUPATEN NATUNA' then '3803'
                        WHEN City = 'KABUPATEN BINTAN' then '3804'
                        WHEN City = 'KABUPATEN KEPULAUAN ANAMBAS' then '3805'
                        WHEN City = 'KOTA TANJUNG PINANG' then '3891'
                        WHEN City = 'KOTA BATAM' then '3892'
                        WHEN City = 'PROVINSI LAMPUNG' then '3900'
                        WHEN City = 'KABUPATEN LAMPUNG SELATAN' then '3901'
                        WHEN City = 'KABUPATEN LAMPUNG TENGAH' then '3902'
                        WHEN City = 'KABUPATEN LAMPUNG UTARA' then '3903'
                        WHEN City = 'KABUPATEN LAMPUNG BARAT' then '3904'
                        WHEN City = 'KABUPATEN TULANG BAWANG' then '3905'
                        WHEN City = 'KABUPATEN TANGGAMUS' then '3906'
                        WHEN City = 'KABUPATEN LAMPUNG TIMUR' then '3907'
                        WHEN City = 'KABUPATEN WAY KANAN' then '3908'
                        WHEN City = 'KABUPATEN PESAWARAN' then '3909'
                        WHEN City = 'KABUPATEN PRINGSEWU' then '3910'
                        WHEN City = 'KABUPATEN TULANG BAWANG BARAT' then '3911'
                        WHEN City = 'KABUPATEN PESISIR BARAT' then '3913'
                        WHEN City = 'KOTA BANDAR LAMPUNG' then '3991'
                        WHEN City = 'KOTA METRO' then '3992'
                        WHEN City = 'KABUPATEN MESUJI' then '3912'
                        WHEN City = 'KABUPATEN BANJAR' then '5101'
                        WHEN City = 'KABUPATEN TANAH LAUT' then '5102'
                        WHEN City = 'KABUPATEN TAPIN' then '5103'
                        WHEN City = 'KABUPATEN HULU SUNGAI SELATAN' then '5104'
                        WHEN City = 'KABUPATEN HULU SUNGAI TENGAH' then '5105'
                        WHEN City = 'KABUPATEN HULU SUNGAI UTARA' then '5106'
                        WHEN City = 'KABUPATEN BARITO KUALA' then '5107'
                        WHEN City = 'KABUPATEN KOTA BARU' then '5108'
                        WHEN City = 'KABUPATEN TABALONG' then '5109'
                        WHEN City = 'KABUPATEN TANAH BUMBU' then '5110'
                        WHEN City = 'KABUPATEN BALANGAN' then '5111'
                        WHEN City = 'KOTA BANJARMASIN' then '5191'
                        WHEN City = 'KOTA BANJARBARU' then '5192'
                        WHEN City = 'PROVINSI KALIMANTAN SELATAN' then '5100'
                        WHEN City = 'PROVINSI KALIMANTAN BARAT' then '5300'
                        WHEN City = 'KABUPATEN PONTIANAK' then '5301'
                        WHEN City = 'KABUPATEN SAMBAS' then '5302'
                        WHEN City = 'KABUPATEN KETAPANG' then '5303'
                        WHEN City = 'KABUPATEN SANGGAU' then '5304'
                        WHEN City = 'KABUPATEN SINTANG' then '5305'
                        WHEN City = 'KABUPATEN KAPUAS HULU' then '5306'
                        WHEN City = 'KABUPATEN BENGKAYANG' then '5307'
                        WHEN City = 'KABUPATEN LANDAK' then '5308'
                        WHEN City = 'KABUPATEN SEKADAU' then '5309'
                        WHEN City = 'KABUPATEN MELAWI' then '5310'
                        WHEN City = 'KABUPATEN KAYONG UTARA' then '5311'
                        WHEN City = 'KABUPATEN KUBU RAYA' then '5312'
                        WHEN City = 'KOTA PONTIANAK' then '5391'
                        WHEN City = 'KOTA SINGKAWANG' then '5392'
                        WHEN City = 'PROVINSI KALIMANTAN TIMUR' then '5400'
                        WHEN City = 'KABUPATEN KUTAI KARTANEGARA' then '5401'
                        WHEN City = 'KABUPATEN BERAU' then '5402'
                        WHEN City = 'KABUPATEN PASER' then '5403'
                        WHEN City = 'KABUPATEN BULUNGAN' then '5404'
                        WHEN City = 'KABUPATEN KUTAI BARAT' then '5405'
                        WHEN City = 'KABUPATEN KUTAI TIMUR' then '5406'
                        WHEN City = 'KABUPATEN NUNUKAN' then '5409'
                        WHEN City = 'KABUPATEN MALINAU' then '5410'
                        WHEN City = 'KABUPATEN PENAJAM PASER UTARA' then '5411'
                        WHEN City = 'KABUPATEN MAHAKAM ULU' then '5413'
                        WHEN City = 'KOTA SAMARINDA' then '5491'
                        WHEN City = 'KOTA BALIKPAPAN' then '5492'
                        WHEN City = 'KOTA TARAKAN' then '5493'
                        WHEN City = 'KOTA BONTANG' then '5494'
                        WHEN City = 'KABUPATEN TANA TIDUNG' then '5412'
                        WHEN City = 'PROVINSI KALIMANTAN TENGAH' then '5800'
                        WHEN City = 'KABUPATEN KAPUAS' then '5801'
                        WHEN City = 'KABUPATEN KOTAWARINGIN BARAT' then '5802'
                        WHEN City = 'KABUPATEN KOTAWARINGIN TIMUR' then '5803'
                        WHEN City = 'KABUPATEN MURUNG RAYA' then '5804'
                        WHEN City = 'KABUPATEN BARITO TIMUR' then '5805'
                        WHEN City = 'KABUPATEN BARITO SELATAN' then '5806'
                        WHEN City = 'KABUPATEN GUNUNG MAS' then '5807'
                        WHEN City = 'KABUPATEN BARITO UTARA' then '5808'
                        WHEN City = 'KABUPATEN PULANG PISAU' then '5809'
                        WHEN City = 'KABUPATEN SERUYAN' then '5810'
                        WHEN City = 'KABUPATEN KATINGAN' then '5811'
                        WHEN City = 'KABUPATEN SUKAMARA' then '5812'
                        WHEN City = 'KABUPATEN LAMANDAU' then '5813'
                        WHEN City = 'KOTA PALANGKARAYA' then '5892'
                        WHEN City = 'PROVINSI SULAWESI TENGAH' then '6000'
                        WHEN City = 'KABUPATEN DONGGALA' then '6001'
                        WHEN City = 'KABUPATEN POSO' then '6002'
                        WHEN City = 'KABUPATEN BANGGAI' then '6003'
                        WHEN City = 'KABUPATEN TOLI-TOLI' then '6004'
                        WHEN City = 'KABUPATEN BANGGAI KEPULAUAN' then '6005'
                        WHEN City = 'KABUPATEN MOROWALI' then '6006'
                        WHEN City = 'KABUPATEN BUOL' then '6007'
                        WHEN City = 'KABUPATEN TOJO UNA-UNA' then '6008'
                        WHEN City = 'KABUPATEN PARIGI MOUTONG' then '6009'
                        WHEN City = 'KABUPATEN SIGI' then '6010'
                        WHEN City = 'KOTA PALU' then '6091'
                        WHEN City = 'KABUPATEN MOROWALI UTARA' then '6012'
                        WHEN City = 'KABUPATEN BANGGAI LAUT' then '6011'
                        WHEN City = 'PROVINSI SULAWESI SELATAN' then '6100'
                        WHEN City = 'KABUPATEN PINRANG' then '6101'
                        WHEN City = 'KABUPATEN GOWA' then '6102'
                        WHEN City = 'KABUPATEN WAJO' then '6103'
                        WHEN City = 'KABUPATEN BONE' then '6105'
                        WHEN City = 'KABUPATEN TANA TORAJA' then '6106'
                        WHEN City = 'KABUPATEN MAROS' then '6107'
                        WHEN City = 'KABUPATEN LUWU' then '6109'
                        WHEN City = 'KABUPATEN SINJAI' then '6110'
                        WHEN City = 'KABUPATEN BULUKUMBA' then '6111'
                        WHEN City = 'KABUPATEN BANTAENG' then '6112'
                        WHEN City = 'KABUPATEN JENEPONTO' then '6113'
                        WHEN City = 'KABUPATEN KEPULAUAN SELAYAR' then '6114'
                        WHEN City = 'KABUPATEN TAKALAR' then '6115'
                        WHEN City = 'KABUPATEN BARRU' then '6116'
                        WHEN City = 'KABUPATEN SIDENRENG RAPPANG' then '6117'
                        WHEN City = 'KABUPATEN PANGKAJENE KEPULAUAN' then '6118'
                        WHEN City = 'KABUPATEN SOPPENG' then '6119'
                        WHEN City = 'KABUPATEN ENREKANG' then '6121'
                        WHEN City = 'KABUPATEN LUWU TIMUR' then '6122'
                        WHEN City = 'KABUPATEN LUWU UTARA' then '6124'
                        WHEN City = 'KABUPATEN TORAJA UTARA' then '6125'
                        WHEN City = 'KOTA MAKASSAR' then '6191'
                        WHEN City = 'KOTA PARE-PARE' then '6192'
                        WHEN City = 'KOTA PALOPO' then '6193'
                        WHEN City = 'PROVINSI SULAWESI UTARA' then '6200'
                        WHEN City = 'KABUPATEN MINAHASA' then '6202'
                        WHEN City = 'KABUPATEN BOLAANG MONGONDOW' then '6203'
                        WHEN City = 'KABUPATEN KEPULAUAN SANGIHE' then '6204'
                        WHEN City = 'KABUPATEN KEPULAUAN TALAUD' then '6205'
                        WHEN City = 'KABUPATEN MINAHASA SELATAN' then '6206'
                        WHEN City = 'KABUPATEN MINAHASA UTARA' then '6207'
                        WHEN City = 'KABUPATEN MINAHASA TENGGARA' then '6209'
                        WHEN City = 'KABUPATEN BOLAANG MONGONDOW UTARA' then '6210'
                        WHEN City = 'KABUPATEN KEPULAUAN SITARO' then '6211'
                        WHEN City = 'KABUPATEN BOLAANG MONGONDOW SELATAN' then '6212'
                        WHEN City = 'KABUPATEN BOLAANG MONGONDOW TIMUR' then '6213'
                        WHEN City = 'KOTA MANADO' then '6291'
                        WHEN City = 'KOTA KOTAMOBAGU' then '6292'
                        WHEN City = 'KOTA BITUNG' then '6293'
                        WHEN City = 'KOTA TOMOHON' then '6294'
                        WHEN City = 'PROVINSI GORONTALO' then '6300'
                        WHEN City = 'KABUPATEN GORONTALO' then '6301'
                        WHEN City = 'KABUPATEN BUALEMO' then '6302'
                        WHEN City = 'KABUPATEN BONEBOLANGO' then '6303'
                        WHEN City = 'KABUPATEN POHUWATO' then '6304'
                        WHEN City = 'KABUPATEN GORONTALO UTARA' then '6305'
                        WHEN City = 'KOTA GORONTALO' then '6391'
                        WHEN City = 'PROVINSI SULAWESI BARAT' then '6400'
                        WHEN City = 'KABUPATEN POLEWALI MANDAR' then '6401'
                        WHEN City = 'KABUPATEN MAJENE' then '6402'
                        WHEN City = 'KABUPATEN MAMASA' then '6403'
                        WHEN City = 'KABUPATEN MAMUJU TENGAH' then '6405'
                        WHEN City = 'KAB MAMUJU' then '6491'
                        WHEN City = 'KABUPATEN MAMUJU UTARA' then '6404'
                        WHEN City = 'KABUPATEN MAMUJU (LBU 2016)' then '6406'
                        WHEN City = 'PROVINSI SULAWESI TENGGARA' then '6900'
                        WHEN City = 'KABUPATEN BUTON' then '6901'
                        WHEN City = 'KABUPATEN MUNA' then '6903'
                        WHEN City = 'KABUPATEN KOLAKA' then '6904'
                        WHEN City = 'KABUPATEN WAKATOBI' then '6905'
                        WHEN City = 'KABUPATEN KONAWE' then '6906'
                        WHEN City = 'KABUPATEN KONAWE SELATAN' then '6907'
                        WHEN City = 'KABUPATEN BOMBANA' then '6908'
                        WHEN City = 'KABUPATEN KOLAKA UTARA' then '6909'
                        WHEN City = 'KABUPATEN BUTON UTARA' then '6910'
                        WHEN City = 'KABUPATEN MUNA BARAT' then '6916'
                        WHEN City = 'KOTA BAU-BAU' then '6990'
                        WHEN City = 'KOTA KENDARI' then '6991'
                        WHEN City = 'KABUPATEN KALOKA TIMUR' then '6912'
                        WHEN City = 'KABUPATEN BUTON TENGAH' then '6915'
                        WHEN City = 'KABUPATEN BUTON SELATAN' then '6914'
                        WHEN City = 'KABUPATEN KONAWE UTARA' then '6911'
                        WHEN City = 'KABUPATEN KONAWE KEPULAUAN' then '6913'
                        WHEN City = 'PROVINSI NUSA TENGGARA BARAT' then '7100'
                        WHEN City = 'KABUPATEN LOMBOK BARAT' then '7101'
                        WHEN City = 'KABUPATEN LOMBOK TENGAH' then '7102'
                        WHEN City = 'KABUPATEN LOMBOK TIMUR' then '7103'
                        WHEN City = 'KABUPATEN SUMBAWA' then '7104'
                        WHEN City = 'KABUPATEN BIMA' then '7105'
                        WHEN City = 'KABUPATEN DOMPU' then '7106'
                        WHEN City = 'KABUPATEN SUMBAWA BARAT' then '7107'
                        WHEN City = 'KABUPATEN LOMBOK UTARA' then '7108'
                        WHEN City = 'KOTA MATARAM' then '7191'
                        WHEN City = 'KOTA BIMA' then '7192'
                        WHEN City = 'PROVINSI BALI' then '7200'
                        WHEN City = 'KABUPATEN BULELENG' then '7201'
                        WHEN City = 'KABUPATEN JEMBRANA' then '7202'
                        WHEN City = 'KABUPATEN TABANAN' then '7203'
                        WHEN City = 'KABUPATEN BADUNG' then '7204'
                        WHEN City = 'KABUPATEN GIANYAR' then '7205'
                        WHEN City = 'KABUPATEN KLUNGKUNG' then '7206'
                        WHEN City = 'KABUPATEN BANGLI' then '7207'
                        WHEN City = 'KABUPATEN KARANGASEM' then '7208'
                        WHEN City = 'KOTA DENPASAR' then '7291'
                        WHEN City = 'PROVINSI NUSA TENGGARA TIMUR' then '7400'
                        WHEN City = 'KABUPATEN KUPANG' then '7401'
                        WHEN City = 'KABUPATEN TIMOR-TENGAH SELATAN' then '7402'
                        WHEN City = 'KABUPATEN TIMOR-TENGAH UTARA' then '7403'
                        WHEN City = 'KABUPATEN BELU' then '7404'
                        WHEN City = 'KABUPATEN ALOR' then '7405'
                        WHEN City = 'KABUPATEN FLORES TIMUR' then '7406'
                        WHEN City = 'KABUPATEN SIKKA' then '7407'
                        WHEN City = 'KABUPATEN ENDE' then '7408'
                        WHEN City = 'KABUPATEN NGADA' then '7409'
                        WHEN City = 'KABUPATEN MANGGARAI' then '7410'
                        WHEN City = 'KABUPATEN SUMBA TIMUR' then '7411'
                        WHEN City = 'KABUPATEN SUMBA BARAT' then '7412'
                        WHEN City = 'KABUPATEN LEMBATA' then '7413'
                        WHEN City = 'KABUPATEN ROTE NDAO' then '7414'
                        WHEN City = 'KABUPATEN MANGGARAI BARAT' then '7415'
                        WHEN City = 'KABUPATEN SUMBA TENGAH' then '7416'
                        WHEN City = 'KABUPATEN SUMBA BARAT DAYA' then '7417'
                        WHEN City = 'KABUPATEN MANGGARAI TIMUR' then '7418'
                        WHEN City = 'KABUPATEN NAGEKEO' then '7419'
                        WHEN City = 'KABUPATEN MALAKA' then '7421'
                        WHEN City = 'KOTA KUPANG' then '7491'
                        WHEN City = 'KABUPATEN SABU RAIJUA' then '7420'
                        WHEN City = 'PROVINSI MALUKU' then '8100'
                        WHEN City = 'KABUPATEN MALUKU TENGAH' then '8101'
                        WHEN City = 'KABUPATEN MALUKU TENGGARA' then '8102'
                        WHEN City = 'KABUPATEN MALUKU TENGGARA BARAT' then '8103'
                        WHEN City = 'KABUPATEN BURU' then '8104'
                        WHEN City = 'KABUPATEN SERAM BAGIAN BARAT' then '8105'
                        WHEN City = 'KABUPATEN SERAM BAGIAN TIMUR' then '8106'
                        WHEN City = 'KABUPATEN KEPULAUAN ARU' then '8107'
                        WHEN City = 'KABUPATEN MALUKU BARAT DAYA' then '8108'
                        WHEN City = 'KABUPATEN BURU SELATAN' then '8109'
                        WHEN City = 'KOTA AMBON' then '8191'
                        WHEN City = 'KOTA TUAL' then '8192'
                        WHEN City = 'PROVINSI PAPUA' then '8200'
                        WHEN City = 'KABUPATEN JAYAPURA' then '8201'
                        WHEN City = 'KABUPATEN BIAK NUMFOR' then '8202'
                        WHEN City = 'KABUPATEN KEPULAUAN YAPEN-WAROPEN' then '8210'
                        WHEN City = 'KABUPATEN MERAUKE' then '8211'
                        WHEN City = 'KABUPATEN PANIAI' then '8212'
                        WHEN City = 'KABUPATEN JAYAWIJAYA' then '8213'
                        WHEN City = 'KABUPATEN NABIRE' then '8214'
                        WHEN City = 'KABUPATEN MIMIKA' then '8215'
                        WHEN City = 'KABUPATEN PUNCAK JAYA' then '8216'
                        WHEN City = 'KABUPATEN SARMI' then '8217'
                        WHEN City = 'KABUPATEN KEEROM' then '8218'
                        WHEN City = 'KABUPATEN PEGUNUNGAN BINTANG' then '8221'
                        WHEN City = 'KABUPATEN YAHUKIMO' then '8222'
                        WHEN City = 'KABUPATEN TOLIKARA' then '8223'
                        WHEN City = 'KABUPATEN WAROPEN' then '8224'
                        WHEN City = 'KABUPATEN BOVEN DIGOEL' then '8226'
                        WHEN City = 'KABUPATEN MAPPI' then '8227'
                        WHEN City = 'KABUPATEN ASMAT' then '8228'
                        WHEN City = 'KABUPATEN SUPIORI' then '8231'
                        WHEN City = 'KABUPATEN MAMBERAMO RAYA' then '8232'
                        WHEN City = 'KABUPATEN DOGIYAI' then '8233'
                        WHEN City = 'KABUPATEN LANNY JAYA' then '8234'
                        WHEN City = 'KABUPATEN MAMBERAMO TENGAH' then '8235'
                        WHEN City = 'KABUPATEN NDUGA' then '8236'
                        WHEN City = 'KABUPATEN YALIMO' then '8237'
                        WHEN City = 'KABUPATEN PUNCAK' then '8238'
                        WHEN City = 'KABUPATEN DEIYA' then '8240'
                        WHEN City = 'KOTA JAYAPURA' then '8291'
                        WHEN City = 'KABUPATEN INTAN JAYA' then '8239'
                        WHEN City = 'PROVINSI MALUKU UTARA' then '8300'
                        WHEN City = 'KABUPATEN HALMAHERA TENGAH' then '8302'
                        WHEN City = 'KABUPATEN HALMAHERA UTARA' then '8303'
                        WHEN City = 'KABUPATEN HALMAHERA TIMUR' then '8304'
                        WHEN City = 'KABUPATEN HALMAHERA BARAT' then '8305'
                        WHEN City = 'KABUPATEN HALMAHERA SELATAN' then '8306'
                        WHEN City = 'KABUPATEN KEPULAUAN SULA' then '8307'
                        WHEN City = 'KABUPATEN PULAU MOROTAI' then '8308'
                        WHEN City = 'KOTA TERNATE' then '8390'
                        WHEN City = 'KOTA TIDORE KEPULAUAN' then '8391'
                        WHEN City = 'KABUPATEN PULAU TALIABU' then '8309'
                        WHEN City = 'PROVINSI PAPUA BARAT' then '8400'
                        WHEN City = 'KABUPATEN SORONG' then '8401'
                        WHEN City = 'KABUPATEN FAK-FAK' then '8402'
                        WHEN City = 'KABUPATEN MANOKWARI' then '8403'
                        WHEN City = 'KABUPATEN SORONG SELATAN' then '8404'
                        WHEN City = 'KABUPATEN RAJA AMPAT' then '8405'
                        WHEN City = 'KABUPATEN KAIMANA' then '8406'
                        WHEN City = 'KABUPATEN TELUK BINTUNI' then '8407'
                        WHEN City = 'KABUPATEN TELUK WONDAMA' then '8408'
                        WHEN City = 'KABUPATEN TEMBRAUW' then '8409'
                        WHEN City = 'KABUPATEN MAYBRAT' then '8410'
                        WHEN City = 'KOTA SORONG' then '8491'
                        WHEN City = 'KABUPATEN PEGUNUNGAN ARFAK' then '8411'
                        WHEN City = 'KABUPATEN MANOKWARI SELATAN' then '8412'
                        WHEN City = 'KABUPATEN BANYU ASIN' then '3607'
                        WHEN City = 'JAKARTA PUSAT' then '0391'
                        WHEN City = 'DI LUAR INDONESIA' then '9999'
                        ELSE '0000'
                    END AS Kode_Kabupaten_atau_Kota,
                    CASE 
                        WHEN length(trim(Kode_Pos)) <> 5 then ''
                        WHEN Kode_Pos is null then ''
                    ELSE Kode_Pos
                    END AS Kode_Pos,
                    Nomor_Telepon,
                    Nomor_Telepon_Seluler,
                    Alamat_email,
                    Kode_Negara_Domisili,
                    Kode_Pekerjaan,
                    LPAD(regexp_replace(regexp_replace(regexp_replace(Tempat_Bekerja, r"([\\n\\r]+)", ' '), r"([\W_] -.,+)", ' '), r" +", ' '), 50) AS Tempat_Bekerja,
                    Kode_Bidang_Usaha_Tempat_Bekerja,
                    LPAD(regexp_replace(regexp_replace(regexp_replace(Alamat_Tempat_Bekerja, r"([\\n\\r]+)", ' '), r"([\W_] -.,+)", ' '), r" +", ' '), 300) AS Alamat_Tempat_Bekerja,
                    Kode_Golongan_Debitur,
                    Status_Perkawinan_Debitur,
                    Kode_Kantor_Cabang,
                    Operasi_Data,
                    Status_delete,
                    FORMAT_TIMESTAMP('%Y%m%d%H%M%S', Create_Date) AS Create_Date,
                    FORMAT_TIMESTAMP('%Y%m%d%H%M%S', Update_Date) AS Update_Date,
                    Approved_Date,
                    userId,
                    orderId,
                    paidoffDate,
                    status
                FROM summary
                WHERE row_number = 1
                order by orderId, Approved_Date
            """.format(date=year_date, filter_q=filter_q)
        
        start_query = time.time()
        query_job = client.query(sql)
        df = query_job.to_dataframe()
        end_query = time.time()
        print("[get_D01_data] Query from BigQuery takes {}".format(end_query-start_query))
        self.D01new = df
        return self.D01new

    def load_dataframe_to_testpandasgbq(self, order_ids):
        client = bigquery.Client(project="athena-179008")
        table_id = 'vayu_data_mart.testpandasgbq'
        job_config = bigquery.LoadJobConfig(schema=[
        #     bigquery.SchemaField("finalScore", "STRING"),
        ],
        write_disposition="WRITE_TRUNCATE",)
        start = time.time()
        data = pd.DataFrame(order_ids).rename(columns={0 : 'orderId'})
        job = client.load_table_from_dataframe(
            data, table_id
            , job_config=job_config
        )
        # Wait for the load job to complete.
        job.result()
        end = time.time()
        print("upload dataframe takes {}".format(end-start))
        return print('Dataframe Successfully Loaded to vayu_data_mart.testpandasgbq')

    def checking_data(self, F01new, D01new, year_month_date, F01_prev=pd.DataFrame({}), D01_prev=pd.DataFrame({})):
        tahun_bulan_data = pd.to_datetime(year_month_date)
        tahun_bulan_data = tahun_bulan_data.strftime("%Y%m")

        if (not F01_prev.empty) & (not D01_prev.empty):
            # Check Previous Not Active Nomor Rekening Fasilitas But Exist in Current Data
            self._print_start_section('Check Previous Not Active Nomor Rekening Fasilitas But Exist in Current Data')

            self.existing_previous_not_active_data = F01new[F01new['Nomor Rekening Fasilitas'].isin(F01_prev[F01_prev['Kode Kondisi']!='00']['Nomor Rekening Fasilitas'].unique())]
            if self.existing_previous_not_active_data['Nomor Rekening Fasilitas'].shape[0] > 0:
                print('Found %d Nomor Rekening Fasilitas'%(self.existing_previous_not_active_data.shape[0]))
                print('--> kbij.existing_previous_not_active_data')

                print('Drop previous not active Nomor Rekening Fasilitas from current F01')
                F01new.drop(self.existing_previous_not_active_data.index.tolist(), inplace=True)

                print('Drop not existing Nomor CIF Debitur in current F01 from current D01')
                D01new.drop(D01new[~D01new['Nomor CIF Debitur'].isin(F01new['Nomor CIF Debitur'].values)].index.tolist(), inplace=True)
            else:
                print('[CLEAR]No Previous Not Active Nomor Rekening Fasilitas But Exist in Current Data')
            self._print_end_section()

            tmp=F01new.join(F01_prev.set_index('Nomor Rekening Fasilitas'), on='Nomor Rekening Fasilitas', how='inner', rsuffix='_prev')
            F01new.loc[F01new['Nomor Rekening Fasilitas'].isin(tmp[tmp['Update Date']!=tmp['Update Date_prev']]['Nomor Rekening Fasilitas']), 'Operasi Data']='U'
            F01new.loc[F01new['Nomor Rekening Fasilitas'].isin(tmp[tmp['Update Date']==tmp['Update Date_prev']]['Nomor Rekening Fasilitas']), 'Operasi Data']='N'

            # Check Previous Active Nomor Rekening Fasilitas But Not Exist in Current Data
            self._print_start_section('Check Previous Active Nomor Rekening Fasilitas But Not Exist in Current Data')
            
            prev_active_df = F01_prev[F01_prev['Kode Kondisi']=='00']
            self.not_exist_previous_active_data = prev_active_df[~prev_active_df['Nomor Rekening Fasilitas'].isin(F01new['Nomor Rekening Fasilitas'].values)]
            if self.not_exist_previous_active_data['Nomor Rekening Fasilitas'].shape[0] > 0:
                print('Found %d Nomor Rekening Fasilitas'%(self.not_exist_previous_active_data.shape[0]))
                print('--> kbij.not_exist_previous_active_data')
            else:
                print('[CLEAR]No Previous Active Nomor Rekening Fasilitas But Not Exist in Current Data')
            self._print_end_section()

        # Check Nomor CIF Debitur in D01 and F01
        self._print_start_section('Check Nomor CIF Debitur in D01 and F01')
        
        self.not_exist_nomor_cif_in_f01 = D01new[~D01new['Nomor CIF Debitur'].isin(F01new['Nomor CIF Debitur'].values)]
        if self.not_exist_nomor_cif_in_f01.shape[0] == 0:
            print('[CLEAR]All Nomor CIF Debitur in D01 are exist in F01')
        else:
            print('Found %d Nomor Rekening Fasilitas'%(self.not_exist_nomor_cif_in_f01.shape[0]))
            print('--> kbij.not_exist_nomor_cif_in_f01')
        self._print_end_section()

        # Check Active Kode Kondisi but Baki Debet = 0
        self._print_start_section('Check Active Kode Kondisi but Baki Debet = 0')
        self.active_but_has_no_balance = F01new[(F01new['Kode Kondisi']== '00') & (F01new['Baki Debet']==0)]
        if self.active_but_has_no_balance.shape[0]>0:
            print('Found %d Nomor Rekening Fasilitas'%(self.active_but_has_no_balance.shape[0]))
            print('--> kbij.active_but_has_no_balance')
            print('Set Kode Kondisi to not active')
            F01new.loc[(F01new['Kode Kondisi']== '00') & (F01new['Baki Debet']==0), 'Tanggal Kondisi'] = F01new.loc[(F01new['Kode Kondisi']== '00') & (F01new['Baki Debet']==0)]['Update Date'].str.slice(stop=8)
            F01new.loc[(F01new['Kode Kondisi']== '00') & (F01new['Baki Debet']==0), 'Denda'] = 0
            F01new.loc[(F01new['Kode Kondisi']== '00') & (F01new['Baki Debet']==0), 'Tunggakan Bunga atau Imbalan'] = 0
            F01new.loc[(F01new['Kode Kondisi']== '00') & (F01new['Baki Debet']==0), 'Tunggakan Pokok'] = 0
            F01new.loc[(F01new['Kode Kondisi']== '00') & (F01new['Baki Debet']==0), 'Jumlah Hari Tunggakan'] = 0
            F01new.loc[(F01new['Kode Kondisi']== '00') & (F01new['Baki Debet']==0), 'Frekuensi Tunggakan'] = 0
            F01new.loc[(F01new['Kode Kondisi']== '00') & (F01new['Baki Debet']==0), 'Operasi Data'] = 'U'
            F01new.loc[(F01new['Kode Kondisi']== '00') & (F01new['Baki Debet']==0), 'Kode Kondisi'] = '02'
        else:
            print('[CLEAR]No Active Kode Kondisi but Baki Debet = 0')
        self._print_end_section()

        # Check Not Active Kode Kondisi but Baki Debet > 0
        self._print_start_section('Check Not Active Kode Kondisi but Baki Debet > 0')
        self.not_active_but_has_balance = F01new[(F01new['Kode Kondisi'] != '00') & (F01new['Baki Debet'] > 0)]
        if self.not_active_but_has_balance.shape[0]>0:
            print('Found %d Nomor Rekening Fasilitas'%(self.not_active_but_has_balance.shape[0]))
            print('--> kbij.not_active_but_has_balance')
        else:
            print('[CLEAR]No Not Active Kode Kondisi but Baki Debet > 0')
        self._print_end_section()

        # Check Baki Debet > Plafon Awal
        self._print_start_section('Check Baki Debet > Plafon Awal')
        self.baki_debet_greater_than_plafon_awal = F01new[(F01new['Baki Debet']>F01new['Plafon Awal'])]
        if self.baki_debet_greater_than_plafon_awal.shape[0]>0:
            print('Found %d Nomor Rekening Fasilitas'%(self.baki_debet_greater_than_plafon_awal.shape[0]))
            print('--> kbij.baki_debet_greater_than_plafon_awal')
        else:
            print('[CLEAR]No Baki Debet greater than Plafon Awal')
        self._print_end_section()

        # Check Kode Kualitas Kredit atau Pembiayaan Macet but has no Tanggal Macet
        self._print_start_section('Check Kode Kualitas Kredit atau Pembiayaan Macet but has no Tanggal Macet')
        self.macet_but_hasno_tanggal_macet = F01new[(F01new['Tanggal Macet'].isnull()) & (F01new['Kode Kualitas Kredit atau Pembiayaan']=='5')]
        if self.macet_but_hasno_tanggal_macet.shape[0]>0:
            print('Found %d Nomor Rekening Fasilitas'%(self.macet_but_hasno_tanggal_macet.shape[0]))
            print('--> kbij.macet_but_hasno_tanggal_macet')
        else:
            print('[CLEAR]No Kode Kualitas Kredit atau Pembiayaan Macet but has no Tanggal Macet')
        self._print_end_section()

        # Check Tahun Bulan Data
        self._print_start_section('Check Tahun Bulan Data')
        print('F01:', F01new['Tahun Bulan Data'].unique().tolist())
        if len(F01new['Tahun Bulan Data'].unique().tolist()) > 1:
            print('Set Tahun Bulan Data to', tahun_bulan_data)
            F01new['Tahun Bulan Data'] = tahun_bulan_data

        print('D01:', D01new['Tahun Bulan Data'].unique().tolist())
        if len(D01new['Tahun Bulan Data'].unique().tolist()) > 1:
            print('Set Tahun Bulan Data to', tahun_bulan_data)
            D01new['Tahun Bulan Data'] = tahun_bulan_data
        self._print_end_section()

        return F01new, D01new