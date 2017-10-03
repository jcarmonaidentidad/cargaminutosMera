//#define DEVELOPER
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.IO;
using System.Data.SqlClient;
using Oracle.DataAccess.Client;
using System.Data.Common;
using System.Net.Mail;

namespace CargaMinutosMera
{
    static class Program
    {
#if DEVELOPER
        public static string SqlStringConnection = "Server=JEREMIASCARMONA;Database=identidad;Trusted_Connection=True;";
       public static StreamWriter logFile = File.AppendText("c:\\Reportes de pruebas\\CargaMinutosMera" + ".log");

       public static string SearchDirectory = "c:\\conectar\\ejemplo_mera\\";
       public static string CopyDirectory = "c:\\conectar\\copia_horas\\";
        
#else
         public static string SqlStringConnection = "Server=DGLT2D42;Database=Identidad;Trusted_Connection=True;";  // Servidor
         public static StreamWriter logFile = File.AppendText("D:\\Intranet\\logs\\CargaMinutosMera" + ".log");

         public static string SearchDirectory = "D:\\Horas_Mera\\"; 
         public static string CopyDirectory = "E:\\Copia_Historico_Mera\\";
#endif
       public static string ReportDateHour;

       public static string fileFound = "";

        public static string comillas = "\"";
        public static string linea_original;

        public static int contador = 0;
     
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        ///   [STAThread]
        static void Main()
        {
          
            logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + "  ");
            logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Inicio proceso ");
          
            SqlConnection conexion = new SqlConnection(SqlStringConnection);
            
            conexion.Open();
            logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + "  Directorio donde se buscan los archivos a leer :" + SearchDirectory);
                //       Directory.SetCurrentDirectory(directory_files);

             // select datename(dw,getdate()) // Preguntar primero por el mes
             // luego preguntar por la semana //
            // luego preguntar por el dia //

             string weekday;

            string horaActual = DateTime.Now.ToString("HH");
                
            SqlConnection sql_connection = new SqlConnection(SqlStringConnection);
            SqlConnection sql_conn_read = new SqlConnection(SqlStringConnection);
            SqlConnection sql_conn_read1 = new SqlConnection(SqlStringConnection);

            SqlConnection upd_conexion = new SqlConnection(SqlStringConnection);

            try
            {

                sql_connection.Open();
                
                sql_conn_read.Open();
                
                sql_conn_read1.Open();

                upd_conexion.Open();

                string[] fileEntries = Directory.GetFiles(SearchDirectory);

                foreach (string fileName in fileEntries)
                {

                    if (fileName.IndexOf(".csv") != -1)
                    {
                        fileFound = "S";

                        string fileToLoad = fileName;

                        string OnlyName = Path.GetFileName(fileToLoad);

                        logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + "  Procesando archivo :" + fileToLoad);

                        StreamReader InputFile = new StreamReader(fileToLoad);

                        string linea_leida = InputFile.ReadLine();

                        string idRecod;
                        string ratePlan;
                        string Customer;
                        string vendor;
                        string area;
                        string rateDescription;
                        string customerRate;
                        string RoutePrefixDescription;
                        string vendorRate;
                        string vendorAccount;
                        string TotalCall;
                        string SeizedCall;
                        string SuccesfullCall;
                        string Duration;
                        string customerChargeablesDuration;
                        string customerPayables;
                        string VendorChargableDuration;
                        string VendorReceivables;
                        string AverageVendorRate;
                        string Margin;
                        
                       
                        int pos_hora = fileToLoad.IndexOf(".csv") - 14;
                        string fechaHora = fileToLoad.Substring(pos_hora, 14);

                        ReportDateHour = OnlyName.Substring(54, 2) + "-" + OnlyName.Substring(56, 2) + "-" + OnlyName.Substring(50, 4) + " 23:59:59";

                        string dateTimeStamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");

                        while ((linea_leida = InputFile.ReadLine()) != null)
                        {
                            try
                            {
                                contador++;
                                linea_original = linea_leida;

                                linea_leida = linea_leida.Replace(comillas, "");
                                linea_leida = linea_leida.Replace("'", "");
                                idRecod = linea_leida.Substring(0, linea_leida.IndexOf(";"));
                                linea_leida = linea_leida.Substring(linea_leida.IndexOf(";") + 1);

                                ratePlan = linea_leida.Substring(0, linea_leida.IndexOf(";"));
                                linea_leida = linea_leida.Substring(linea_leida.IndexOf(";") + 1);

                                Customer = linea_leida.Substring(0, linea_leida.IndexOf(";"));
                                linea_leida = linea_leida.Substring(linea_leida.IndexOf(";") + 1);

                                if (Customer.Trim().Length != 0)
                                {

                                    vendor = linea_leida.Substring(0, linea_leida.IndexOf(";"));
                                    linea_leida = linea_leida.Substring(linea_leida.IndexOf(";") + 1);

                                    if (vendor.Trim().Length != 0)
                                    {
                                        area = linea_leida.Substring(0, linea_leida.IndexOf(";"));
                                        linea_leida = linea_leida.Substring(linea_leida.IndexOf(";") + 1);

                                        rateDescription = linea_leida.Substring(0, linea_leida.IndexOf(";"));
                                        linea_leida = linea_leida.Substring(linea_leida.IndexOf(";") + 1);

                                        customerRate = linea_leida.Substring(0, linea_leida.IndexOf(";"));
                                        linea_leida = linea_leida.Substring(linea_leida.IndexOf(";") + 1);

                                        RoutePrefixDescription = linea_leida.Substring(0, linea_leida.IndexOf(";"));
                                        linea_leida = linea_leida.Substring(linea_leida.IndexOf(";") + 1);

                                        vendorRate = linea_leida.Substring(0, linea_leida.IndexOf(";"));
                                        linea_leida = linea_leida.Substring(linea_leida.IndexOf(";") + 1);

                                        vendorAccount = linea_leida.Substring(0, linea_leida.IndexOf(";"));
                                        linea_leida = linea_leida.Substring(linea_leida.IndexOf(";") + 1);

                                        TotalCall = linea_leida.Substring(0, linea_leida.IndexOf(";"));
                                        linea_leida = linea_leida.Substring(linea_leida.IndexOf(";") + 1);

                                        SeizedCall = linea_leida.Substring(0, linea_leida.IndexOf(";"));
                                        linea_leida = linea_leida.Substring(linea_leida.IndexOf(";") + 1);

                                        SuccesfullCall = linea_leida.Substring(0, linea_leida.IndexOf(";"));
                                        linea_leida = linea_leida.Substring(linea_leida.IndexOf(";") + 1);

                                        Duration = linea_leida.Substring(0, linea_leida.IndexOf(";"));
                                        linea_leida = linea_leida.Substring(linea_leida.IndexOf(";") + 1);

                                        customerChargeablesDuration = linea_leida.Substring(0, linea_leida.IndexOf(";"));
                                        linea_leida = linea_leida.Substring(linea_leida.IndexOf(";") + 1);

                                        customerPayables = linea_leida.Substring(0, linea_leida.IndexOf(";"));
                                        linea_leida = linea_leida.Substring(linea_leida.IndexOf(";") + 1);

                                        VendorChargableDuration = linea_leida.Substring(0, linea_leida.IndexOf(";"));
                                        linea_leida = linea_leida.Substring(linea_leida.IndexOf(";") + 1);

                                        VendorReceivables = linea_leida.Substring(0, linea_leida.IndexOf(";"));
                                        linea_leida = linea_leida.Substring(linea_leida.IndexOf(";") + 1);

                                        AverageVendorRate = linea_leida.Substring(0, linea_leida.IndexOf(";"));
                                        linea_leida = linea_leida.Substring(linea_leida.IndexOf(";") + 1);

                                        Margin = linea_leida.Substring(0);
                                        Margin = Margin.Replace(",", "");

                                        if (TotalCall != "0")
                                        {

                                            if (customerRate.Trim().Length == 0)
                                            {
                                                customerRate = "0";
                                            }

                                            if (vendorRate.Trim().Length == 0)
                                            {
                                                vendorRate = "0";
                                            }
                                            if (TotalCall.Trim().Length == 0)
                                            {
                                                TotalCall = "0";
                                            }
                                            if (SeizedCall.Trim().Length == 0)
                                            {
                                                SeizedCall = "0";
                                            }
                                            if (SuccesfullCall.Trim().Length == 0)
                                            {
                                                SuccesfullCall = "0";
                                            }
                                            if (Duration.Trim().Length == 0)
                                            {
                                                Duration = "0";
                                            }
                                            if (customerChargeablesDuration.Trim().Length == 0)
                                            {
                                                customerChargeablesDuration = "0";
                                            }
                                            if (customerPayables.Trim().Length == 0)
                                            {
                                                customerPayables = "0";
                                            }
                                            if (VendorChargableDuration.Trim().Length == 0)
                                            {
                                                VendorChargableDuration = "0";
                                            }
                                            if (VendorReceivables.Trim().Length == 0)
                                            {
                                                VendorReceivables = "0";
                                            }
                                            if (AverageVendorRate.Trim().Length == 0)
                                            {
                                                AverageVendorRate = "0";
                                            }
                                            if (Margin.Trim().Length == 0)
                                            {
                                                Margin = "0";
                                            }

                                           
                                            string sqlInsertaMinutos = "insert into Days_mera.dbo.day_history values ('" + Customer + "','" + vendor + "','" + area + "','" + rateDescription + "'," + customerRate + ",'" +
                                                                RoutePrefixDescription + "'," + vendorRate + "," + TotalCall + "," + SeizedCall + "," + SuccesfullCall + "," +
                                                                Duration + "," + customerChargeablesDuration + "," + customerPayables + "," + VendorChargableDuration + "," + VendorReceivables + "," + AverageVendorRate + "," + Margin + ",'" + ReportDateHour + "','" + dateTimeStamp + "')";
                                            SqlCommand cmdInsertaMinutos = new SqlCommand(sqlInsertaMinutos, sql_connection);

                                            try
                                            {
                                                int resInserta = cmdInsertaMinutos.ExecuteNonQuery();

                                            }
                                            catch (Exception E)
                                            {
                                                logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Linea Leida =" + contador + "  Error al insertar registro in day_history, sql=" + sqlInsertaMinutos + " " + E.Message);
                                            }
                                        }
                                    }
                                }
                            }
                            catch (Exception E)
                            {
                                logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Linea Leida="+contador+"  Registro no procesado =" + linea_original + " " + E.Message);
                            }
                        }

                        InputFile.Close();
                       
                        logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Archivo Procesado "+fileToLoad);
                        try
                        {
                            File.Move(fileToLoad, CopyDirectory + OnlyName);
                             
                        }
                        catch (Exception E)
                        {
                            if (E.Message.IndexOf("exists") != -1)
                            {
                                logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Archivo ya existe =" + fileToLoad);
                                File.Delete(fileToLoad);
                            }
                            else
                            {
                                logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Error al mover el archivo =" + fileToLoad + " " + E.Message);
                            }
                        }
                        logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Acumular bilateral ");
                        AcumulaBilateral();                       
                        logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Acumular bilateral Ayer ");
                        AcumulaBilateralAyer();
                        GetCostAvg();
                        LlenaTemporal();                     
                    }
                } // Termino con el archivo ahora a acumular del dia 
                if (fileFound !="S")
                {
                    // Envia Email NO file where process today //
                    EnviaEmail();
                }
            
            }
            catch (Exception e)
            {
                logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + "  Error durante el proceso de carga, linea_leida ="+   linea_original+"  message=" + e.Message);
                EnviaEmail();
            }
            finally
            {
                sql_connection.Close();
                upd_conexion.Close();
                sql_conn_read.Close();
                sql_conn_read1.Close();

                logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Fin proceso ");
                logFile.Close();
            }
            
        }


        static void AcumulaDia()
        {

            SqlConnection updConexion = new SqlConnection(SqlStringConnection);

            SqlConnection sqlConnRead = new SqlConnection(SqlStringConnection);

            SqlConnection sqlConnRead1 = new SqlConnection(SqlStringConnection);

            updConexion.Open();
            sqlConnRead.Open();
            sqlConnRead1.Open();

            string sqlGetHour = " select  customer, vendor, Area, Customer_Area_Description, customer_rate, vendor_area_description, vendor_rate, " +
                                    " sum(TotalCalls) 'TotalCalls', sum(SeizedCall) 'SeizedCall', sum(SuccesfullCall) 'SuccesFullCall', sum(duration) 'duration', " +
                                    " sum(customerchargeablesDuration) 'customerchargeablesDuration', sum(customerPayables) 'customerpayables', sum(vendorchargableDuration) 'vendorchargableduration', " +
                                    " sum(vendorReceivables) 'vendorReceivables', sum(averageVendorRate) 'averageVendorRate', sum(margin) 'Margin' " +
                                    " from Days_mera.dbo.Horas_dia hr " +
                                    " where ReportDateHour ='" + ReportDateHour +
//                                    "' and duration is not null " +
                                    "' and totalCalls <>0  " +
                                    " group by Customer, vendor, area, customer_area_Description, customer_rate, Vendor_area_description, vendor_rate ";

            SqlCommand cmdGetHour = new SqlCommand(sqlGetHour, sqlConnRead);

            cmdGetHour.CommandTimeout = 1200;

            SqlDataReader readHour = cmdGetHour.ExecuteReader();
            if (readHour.HasRows)
            {
                while (readHour.Read())
                {
                    string customer = readHour["customer"].ToString();
                    string vendor1 = readHour["vendor"].ToString();
                    string area1 = readHour["Area"].ToString();
                    string customer_area_description = readHour["Customer_Area_Description"].ToString();
                    string customer_rate = readHour["customer_rate"].ToString();
                    string vendor_area_description = readHour["vendor_area_description"].ToString();
                    string vendor_rate = readHour["vendor_rate"].ToString();
                    string total_call = readHour["TotalCalls"].ToString();
                    string SeizedCall1 = readHour["SeizedCall"].ToString();
                    string SuccesFullCall = readHour["SuccesFullCall"].ToString();
                    string duration = readHour["duration"].ToString();
                    string margin = readHour["margin"].ToString();
                    if (duration.Trim().Length  == 0)
                    {
                        duration = "0";
                    }
                    string customerchargeablesDuration = readHour["customerchargeablesDuration"].ToString();
                    if (customerchargeablesDuration.Trim().Length == 0)
                    {
                        customerchargeablesDuration = "0";
                    }
                    string customerpayables = readHour["customerpayables"].ToString();
                    if (customerpayables.Trim().Length ==0)
                    {
                        customerpayables = "0";
                    }
                    string vendorchargableduration = readHour["vendorchargableduration"].ToString();
                    if (vendorchargableduration.Trim().Length ==0)
                    {
                        vendorchargableduration = "0";
                    }
                    string vendorReceivables = readHour["vendorReceivables"].ToString();
                    if (vendorReceivables.Trim().Length ==0)
                    {
                        vendorReceivables = "0";
                    }
                    string averageVendorRate = readHour["averageVendorRate"].ToString();
                    if (averageVendorRate.Trim().Length ==0)
                    {
                        averageVendorRate = "0";
                    }
                    if (customer_rate.Trim().Length == 0)
                    {
                        customer_rate = "0";
                    }
                    if (vendor_rate.Trim().Length == 0)
                    {
                        vendor_rate = "0";
                    }
                    string Margin1 = readHour["Margin"].ToString();

                    string sqlGetoUpdate = "select * from Days_mera.dbo.Dia_actual_acum " +
                                    "where customer='" + customer + "' " +
                                    " and vendor ='" + vendor1 + "' " +
                                    " and area = '" + area1 + "' " +
                                    " and customer_area_description='" + customer_area_description + "' " +
                                    " and customer_rate ='" + customer_rate + "' " +
                                    " and vendor_area_description ='" + vendor_area_description + "' " +
                                    " and vendor_rate ='" + vendor_rate + "' ";

                    SqlCommand cmdGetToUpdate = new SqlCommand(sqlGetoUpdate, sqlConnRead1);
                    SqlDataReader readToUpdate = cmdGetToUpdate.ExecuteReader();

                    if (readToUpdate.HasRows)
                    {

                        string sqlUpdRecord = "update Days_mera.dbo.Dia_actual_acum set TotalCalls = TotalCalls +" + Convert.ToInt64(total_call) + "," +
                                               " SeizedCall =SeizedCall +" + Convert.ToInt64(SeizedCall1) + "," +
                                               " SuccesFullCall = SuccesFullCall +" + Convert.ToInt64(SuccesFullCall) + "," +
                                               " duration = duration +" + Convert.ToDecimal(duration) + ", " +
                                               " customerchargeablesDuration = customerchargeablesDuration +" + Convert.ToDecimal(customerchargeablesDuration) + "," +
                                               " customerpayables = customerpayables +" + Convert.ToDecimal(customerpayables) + "," +
                                               " vendorchargableduration = vendorchargableduration +" + Convert.ToDecimal(vendorchargableduration) + "," +
                                               " vendorReceivables = vendorReceivables +" + Convert.ToDecimal(vendorReceivables) + "," +
                                               " averageVendorRate= averageVendorRate +" + Convert.ToDecimal(averageVendorRate) + "," +
                                               " Margin =Margin +" + Convert.ToDecimal(margin) +
                                               "where customer='" + customer + "' " +
                                               " and vendor ='" + vendor1 + "' " +
                                               " and area = '" + area1 + "' " +
                                               " and customer_area_description='" + customer_area_description + "' " +
                                               " and customer_rate ='" + customer_rate + "' " +
                                               " and vendor_area_description ='" + vendor_area_description + "' " +
                                               " and vendor_rate ='" + vendor_rate + "' ";

                        SqlCommand cmdUpdRecord = new SqlCommand(sqlUpdRecord, updConexion);

                        try
                        {
                            int resultUpd = cmdUpdRecord.ExecuteNonQuery();
                        }
                        catch (Exception E)
                        {
                            logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Error Actualizando registros  " + sqlUpdRecord + " " + E.Message);
                        }

                    }
                    else // No esta el registro, so se va a insertar //
                    {
                        string sqlNewRecord = " insert into Days_mera.dbo.Dia_actual_acum values ('" + customer + "','" + vendor1 + "','" + area1 + "','" + customer_area_description + "','" +
                                                        customer_rate + "','" + vendor_area_description + "','" + vendor_rate + "','" + total_call + "','" +
                                                        SeizedCall1 + "','" + SuccesFullCall + "','" + duration + "','" + customerchargeablesDuration + "','" +
                                                        customerpayables + "','" + vendorchargableduration + "','" + vendorReceivables + "','" + averageVendorRate + "','" +
                                                        Margin1 + "','" + ReportDateHour + "','" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "') ";
                        SqlCommand cmdNewRecord = new SqlCommand(sqlNewRecord, updConexion);

                        try
                        {
                            int resultNew = cmdNewRecord.ExecuteNonQuery();
                        }
                        catch (Exception E)
                        {
                            logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Error al insertar nuevo registro  " + sqlNewRecord + " " + E.Message);
                        }
                    }
                    readToUpdate.Close();
                }
            }
            else
            {
                logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Error durante el proceso de actualizacion Acumulado, no trajo registros ");
            }
            readHour.Close();

            updConexion.Close();
            sqlConnRead.Close();
            sqlConnRead1.Close();
        }
        static void diaActualToDiaAnterior()
        {
            // Me quede aqui //1
            string sql_clean = "truncate table days_mera.dbo.Dia_anterior_acum ";
            SqlConnection con_clean = new SqlConnection(SqlStringConnection);

            con_clean.Open();
            SqlCommand cmd_clean = new SqlCommand(sql_clean, con_clean);

            try
            {
                int result_clean = cmd_clean.ExecuteNonQuery();
            }
            catch (Exception E)
            {
                logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Error Borrando Tabla dia_anterior_acum  " + sql_clean + " " + E.Message);
            }
            // pasa los acumulados del dia a Acumulados del dia anterior //

            string sql_insert = "insert into Days_mera.dbo.Dia_anterior_acum (customer, vendor, Area, Customer_Area_Description, Customer_Rate, Vendor_Area_Description, " +
                                    "Vendor_Rate, TotalCalls, SeizedCall, Duration, CustomerChargeablesDuration, CustomerPayables, VendorChargableDuration,"+
                                    "VendorReceivables, AverageVendorRate, Margin, ReportDateHour, date_time_stamp) "+
                                    "select customer, vendor, Area, Customer_Area_Description, Customer_Rate, Vendor_Area_Description, "+
                                    "Vendor_Rate, TotalCalls, SeizedCall, Duration, CustomerChargeablesDuration, CustomerPayables, VendorChargableDuration, "+
                                    "VendorReceivables, AverageVendorRate, Margin, ReportDateHour, date_time_stamp  from Days_mera.dbo.Dia_actual_acum ";

            SqlCommand cmd_insert = new SqlCommand(sql_insert, con_clean);

            try
            {
                int result_insert = cmd_insert.ExecuteNonQuery();

                // Inserta en historico de dias acumulados //

        //        string sql_insert_hist = "insert into Days_mera.dbo.day_history (customer, vendor, Area, Customer_Area_Description, Customer_Rate, Vendor_Area_Description, " +
        //                            "Vendor_Rate, TotalCalls, SeizedCall, Duration, CustomerChargeablesDuration, CustomerPayables, VendorChargableDuration," +
        //                            "VendorReceivables, AverageVendorRate, Margin, ReportDateHour, date_time_stamp) " +
        //                            "select customer, vendor, Area, Customer_Area_Description, Customer_Rate, Vendor_Area_Description, " +
        //                            "Vendor_Rate, TotalCalls, SeizedCall, Duration, CustomerChargeablesDuration, CustomerPayables, VendorChargableDuration, " +
       //                             "VendorReceivables, AverageVendorRate, Margin, ReportDateHour, date_time_stamp  from Days_mera.dbo.Dia_actual_acum ";

       //         SqlCommand cmd_insert_hist = new SqlCommand(sql_insert_hist, con_clean);
                try
                {
       //             int result_hist = cmd_insert_hist.ExecuteNonQuery();

                    try
                    {
                        string sql_empty = "truncate table Days_mera.dbo.Dia_actual_acum";
                        SqlCommand cmd_empty = new SqlCommand(sql_empty, con_clean);

                        try
                        {
                            int result_empty = cmd_empty.ExecuteNonQuery();  // Luego se pasa a la semana actual //
                            string sql_clean_horas_dia = "truncate table Days_mera.dbo.Horas_dia";
                            SqlCommand cmd_clean_horas_dia = new SqlCommand(sql_clean_horas_dia, con_clean);

                            try
                            {
                                int result_clean_dia = cmd_clean_horas_dia.ExecuteNonQuery();
                            }
                            catch (Exception E)
                            {
                                logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Error Borrando Tabla Horas_dia  " + sql_clean_horas_dia + " " + E.Message);
                            }
                        }
                        catch (Exception E)
                        {
                            logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Error Borrando Tabla dia_actual_acum  " + sql_empty + " " + E.Message);
                        }
                    }
                    catch (Exception E)
                    {
                        logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Error Borrando la Tabla dia_anterior_acum  " + sql_clean + " " + E.Message); 
                    }
                    con_clean.Close();

                }
                catch (Exception E)
                {
                    logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Error Insertando en la Tabla history day  " + sql_clean + " " + E.Message);
                }
            }
            catch (Exception E)
            {
                logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Error Insertando en acumulado del dia anterior " + sql_clean + " " + E.Message);
           }
        }
        static void semanaActualToSemanaAnterior()
        {
            SqlConnection con_weeks = new SqlConnection(SqlStringConnection);
            con_weeks.Open();
            string sql_semana_actual = "insert into Weeks_mera.dbo.semana_anterior_acum (customer, vendor, Area, Customer_Area_Description, Customer_Rate, Vendor_Area_Description,"+ 
                                    " Vendor_Rate, TotalCalls, SeizedCall, Duration, CustomerChargeablesDuration, CustomerPayables, VendorChargableDuration, "+
                                    " VendorReceivables, AverageVendorRate, Margin, ReportDateHour, date_time_stamp) select Customer, vendor, Area, Customer_Area_Description, "+
                                    "customer_rate, vendor_area_description, vendor_rate, TotalCalls, SeizedCall, SuccesfullCall, duration, "+
                                    " customerchargeablesDuration customerPayables, vendorchargableDuration, vendorReceivables , averageVendorRate, "+
                                    " margin, convert(varchar,getdate()-1, 111) as reportDateHour, getdate() as date_time_stamp from Weeks_mera.dbo.semana_actual_acum ";
            SqlCommand cmd_semana_actual = new SqlCommand(sql_semana_actual, con_weeks);

            try
            {
                int result = cmd_semana_actual.ExecuteNonQuery();
                // Se cambio el dia 03/07/2016 para que vaya acumulando por horas// a solicitud de Andres //

/*                string sql_mes_actual = "insert into Months_mera.dbo.month_actual_acum (customer, vendor, Area, Customer_Area_Description, Customer_Rate, Vendor_Area_Description," +
                                   " Vendor_Rate, TotalCalls, SeizedCall, Duration, CustomerChargeablesDuration, CustomerPayables, VendorChargableDuration, " +
                                   " VendorReceivables, AverageVendorRate, Margin, ReportDateHour, date_time_stamp) select Customer, vendor, Area, Customer_Area_Description, " +
                                   "customer_rate, vendor_area_description, vendor_rate, TotalCalls, SeizedCall, SuccesfullCall, duration, " +
                                   " customerchargeablesDuration customerPayables, vendorchargableDuration, vendorReceivables , averageVendorRate, " +
                                   " margin, convert(varchar,getdate()-1, 111) as reportDateHour, getdate() as date_time_stamp from Weeks_mera.dbo.semana_anterior_acum ";
                SqlCommand cmd_mes_actual = new SqlCommand(sql_mes_actual, con_weeks);

                try
                {
                    int result_me = cmd_mes_actual.ExecuteNonQuery();
                }
                catch (Exception E)
                {
                    logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Error al pasar el acumulado de la semana anterior al mes actual, query=" + cmd_mes_actual + "  " + E.Message);
                }
                */
            }
            catch (Exception E)
            {
                logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Error al pasar el acumulado de la semana actual a la semana anterior, query="+sql_semana_actual+"  "+E.Message);
            }
            con_weeks.Close();
        }
        static void mesActual_to_mesAnterior()
        {
            SqlConnection con_months = new SqlConnection(SqlStringConnection);
            con_months.Open();
            string sql_mes_anterior_to_history = "insert into Months_mera.dbo.month_history (customer, vendor, Area, Customer_Area_Description, Customer_Rate, Vendor_Area_Description,"+ 
                                    " Vendor_Rate, TotalCalls, SeizedCall, Duration, CustomerChargeablesDuration, CustomerPayables, VendorChargableDuration, "+
                                    " VendorReceivables, AverageVendorRate, Margin, ReportDateHour, date_time_stamp) select Customer, vendor, Area, Customer_Area_Description, "+
                                    "customer_rate, vendor_area_description, vendor_rate, TotalCalls, SeizedCall, SuccesfullCall, duration, "+
                                    " customerchargeablesDuration customerPayables, vendorchargableDuration, vendorReceivables , averageVendorRate, "+
                                    " margin, convert(varchar,getdate()-1, 111) as reportDateHour, getdate() as date_time_stamp from Months_mera.dbo.month_anterior_acum ";
            SqlCommand cmd_mes_anterior_to_history = new SqlCommand(sql_mes_anterior_to_history, con_months);

            try
            {
                int result = cmd_mes_anterior_to_history.ExecuteNonQuery();
                // Luego que se pasa al historico se pasa lo del mes actual a mes anterior //
                string sql_mes_actual_to_mes_anterior = "insert into Months_mera.dbo.month_anterior_acum (customer, vendor, Area, Customer_Area_Description, Customer_Rate, Vendor_Area_Description," + 
                                    " Vendor_Rate, TotalCalls, SeizedCall, Duration, CustomerChargeablesDuration, CustomerPayables, VendorChargableDuration, "+
                                    " VendorReceivables, AverageVendorRate, Margin, ReportDateHour, date_time_stamp) select Customer, vendor, Area, Customer_Area_Description, "+
                                    "customer_rate, vendor_area_description, vendor_rate, TotalCalls, SeizedCall, SuccesfullCall, duration, "+
                                    " customerchargeablesDuration customerPayables, vendorchargableDuration, vendorReceivables , averageVendorRate, "+
                                    " margin, convert(varchar,getdate()-1, 111) as reportDateHour, getdate() as date_time_stamp from Months_mera.dbo.month_actual_acum ";
                SqlCommand cmd_mes_actual_to_mes_anterior = new SqlCommand(sql_mes_actual_to_mes_anterior, con_months);

                try
                {
                    cmd_mes_actual_to_mes_anterior.ExecuteNonQuery();
                    // Si quedo bien el mes actual to mes anteror se limpia el mes actual para comenzar a llenarse de nuevo //
                    string sql_clean_table = "truncate table Months_mera.dbo.month_actual_acum ";
                    SqlCommand cmd_clean_table = new SqlCommand(sql_clean_table, con_months);

                    try
                    {
                        cmd_clean_table.ExecuteNonQuery();
                    }
                    catch (Exception E)
                    {
                        logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Error al limpimar tabla mes actual, query=" + sql_clean_table + "  " + E.Message);
                    }
                }
                catch (Exception E)
                {
                    logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Error al pasar el acumulado del mes actual al mes anterior, query=" + sql_mes_actual_to_mes_anterior + "  " + E.Message);
                }

            }
            catch (Exception E)
            {
                logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Error al pasar el acumulado del mes anterior al historico de meses, query=" + sql_mes_anterior_to_history + "  " + E.Message);
            }
            con_months.Close();

        }
        static void acumulaMes()
        {
            SqlConnection conexion_month_update = new SqlConnection(SqlStringConnection);
            conexion_month_update.Open();

            SqlConnection conexion_month_read = new SqlConnection(SqlStringConnection);
            conexion_month_read.Open();

            SqlConnection conexion_hour_read = new SqlConnection(SqlStringConnection);
            conexion_hour_read.Open();

            string sql_get_hour = " select  customer, vendor, Area, Customer_Area_Description, customer_rate, vendor_area_description, vendor_rate, " +
                                    " sum(TotalCalls) 'TotalCalls', sum(SeizedCall) 'SeizedCall', sum(SuccesfullCall) 'SuccesFullCall', sum(duration) 'duration', " +
                                    " sum(customerchargeablesDuration) 'customerchargeablesDuration', sum(customerPayables) 'customerpayables', sum(vendorchargableDuration) 'vendorchargableduration', " +
                                    " sum(vendorReceivables) 'vendorReceivables', sum(averageVendorRate) 'averageVendorRate', sum(margin) 'Margin' " +
                                    " from Days_mera.dbo.dia_actual_acum hr " +
                                    " where ReportDateHour ='" + ReportDateHour +
                //                                    "' and duration is not null " +
                                    "' and totalCalls <>0  " +
                                    " group by Customer, vendor, area, customer_area_Description, customer_rate, Vendor_area_description, vendor_rate ";

            SqlCommand cmd_get_hour = new SqlCommand(sql_get_hour, conexion_hour_read);

            SqlDataReader read_hour = cmd_get_hour.ExecuteReader();
            if (read_hour.HasRows)
            {
                while (read_hour.Read())
                {
                    string customer = read_hour["customer"].ToString();
                    string vendor1 = read_hour["vendor"].ToString();
                    string area1 = read_hour["Area"].ToString();
                    string customer_area_description = read_hour["Customer_Area_Description"].ToString();
                    string customer_rate = read_hour["customer_rate"].ToString();
                    string vendor_area_description = read_hour["vendor_area_description"].ToString();
                    string vendor_rate = read_hour["vendor_rate"].ToString();
                    string total_call = read_hour["TotalCalls"].ToString();
                    string SeizedCall1 = read_hour["SeizedCall"].ToString();
                    string SuccesFullCall = read_hour["SuccesFullCall"].ToString();
                    string duration = read_hour["duration"].ToString();
                    string margin = read_hour["margin"].ToString();
                    if (duration.Trim().Length == 0)
                    {
                        duration = "0";
                    }
                    if (margin.Trim().Length ==0)
                    {
                        margin = "0";
                    }
                    string customerchargeablesDuration = read_hour["customerchargeablesDuration"].ToString();
                    if (customerchargeablesDuration.Trim().Length == 0)
                    {
                        customerchargeablesDuration = "0";
                    }
                    string customerpayables = read_hour["customerpayables"].ToString();
                    if (customerpayables.Trim().Length == 0)
                    {
                        customerpayables = "0";
                    }
                    string vendorchargableduration = read_hour["vendorchargableduration"].ToString();
                    if (vendorchargableduration.Trim().Length == 0)
                    {
                        vendorchargableduration = "0";
                    }
                    string vendorReceivables = read_hour["vendorReceivables"].ToString();
                    if (vendorReceivables.Trim().Length == 0)
                    {
                        vendorReceivables = "0";
                    }
                    string averageVendorRate = read_hour["averageVendorRate"].ToString();
                    if (averageVendorRate.Trim().Length == 0)
                    {
                        averageVendorRate = "0";
                    }
                    if (customer_rate.Trim().Length == 0)
                    {
                        customer_rate = "0";
                    }
                    if (vendor_rate.Trim().Length == 0)
                    {
                        vendor_rate = "0";
                    }
                    string Margin1 = read_hour["Margin"].ToString();

                    string sql_get_toUpdate = "select * from Months_mera.dbo.month_actual_acum " +
                                    "where customer='" + customer + "' " +
                                    " and vendor ='" + vendor1 + "' " +
                                    " and area = '" + area1 + "' " +
                                    " and customer_area_description='" + customer_area_description + "' " +
                                    " and customer_rate ='" + customer_rate + "' " +
                                    " and vendor_area_description ='" + vendor_area_description + "' " +
                                    " and vendor_rate ='" + vendor_rate + "' ";

                    SqlCommand cmd_get_toUpdate = new SqlCommand(sql_get_toUpdate, conexion_month_read);
                    SqlDataReader read_toUpdate = cmd_get_toUpdate.ExecuteReader();

                    if (read_toUpdate.HasRows)
                    {

                        string sql_upd_record = "update Months_mera.dbo.month_actual_acum set TotalCalls = TotalCalls +" + Convert.ToInt64(total_call) + "," +
                                               " SeizedCall =SeizedCall +" + Convert.ToInt64(SeizedCall1) + "," +
                                               " SuccesFullCall = SuccesFullCall +" + Convert.ToInt64(SuccesFullCall) + "," +
                                               " duration = duration +" + Convert.ToDecimal(duration) + ", " +
                                               " customerchargeablesDuration = customerchargeablesDuration +" + Convert.ToDecimal(customerchargeablesDuration) + "," +
                                               " customerpayables = customerpayables +" + Convert.ToDecimal(customerpayables) + "," +
                                               " vendorchargableduration = vendorchargableduration +" + Convert.ToDecimal(vendorchargableduration) + "," +
                                               " vendorReceivables = vendorReceivables +" + Convert.ToDecimal(vendorReceivables) + "," +
                                               " averageVendorRate= averageVendorRate +" + Convert.ToDecimal(averageVendorRate) + "," +
                                               " Margin =Margin +" + Convert.ToDecimal(margin) +
                                               "where customer='" + customer + "' " +
                                               " and vendor ='" + vendor1 + "' " +
                                               " and area = '" + area1 + "' " +
                                               " and customer_area_description='" + customer_area_description + "' " +
                                               " and customer_rate ='" + customer_rate + "' " +
                                               " and vendor_area_description ='" + vendor_area_description + "' " +
                                               " and vendor_rate ='" + vendor_rate + "' ";

                        SqlCommand cmd_upd_record = new SqlCommand(sql_upd_record, conexion_month_update);

                        try
                        {
                            int result_upd = cmd_upd_record.ExecuteNonQuery();
                        }
                        catch (Exception E)
                        {
                            logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Error Actualizando registros, en la tabla de acumulados por mes" + sql_upd_record + " " + E.Message);
                        }

                    }
                    else // No esta el registro, so se va a insertar //
                    {
                        string sql_new_record = " insert into Months_mera.dbo.month_actual_acum  values ('" + customer + "','" + vendor1 + "','" + area1 + "','" + customer_area_description + "','" +
                                                        customer_rate + "','" + vendor_area_description + "','" + vendor_rate + "','" + total_call + "','" +
                                                        SeizedCall1 + "','" + SuccesFullCall + "','" + duration + "','" + customerchargeablesDuration + "','" +
                                                        customerpayables + "','" + vendorchargableduration + "','" + vendorReceivables + "','" + averageVendorRate + "','" +
                                                        Margin1 + "','" + ReportDateHour + "','" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "') ";
                        SqlCommand cmd_new_record = new SqlCommand(sql_new_record, conexion_month_update);

                        try
                        {
                            int result_new = cmd_new_record.ExecuteNonQuery();
                        }
                        catch (Exception E)
                        {
                            logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Error al insertar nuevo registro en la tabla de acumulados por mes " + sql_new_record + " " + E.Message);
                        }
                    }
                    read_toUpdate.Close();
                }
            }
            conexion_hour_read.Close();
            conexion_month_read.Close();
            conexion_month_update.Close();

        }
        static void acumulaSemana()
        {
            SqlConnection conexion_week_update = new SqlConnection(SqlStringConnection);
            conexion_week_update.Open();

            SqlConnection conexion_week_read = new SqlConnection(SqlStringConnection);
            conexion_week_read.Open();

            SqlConnection conexion_hour_read = new SqlConnection(SqlStringConnection);
            conexion_hour_read.Open();

            string sql_get_hour = " select  customer, vendor, Area, Customer_Area_Description, customer_rate, vendor_area_description, vendor_rate, " +
                                    " sum(TotalCalls) 'TotalCalls', sum(SeizedCall) 'SeizedCall', sum(SuccesfullCall) 'SuccesFullCall', sum(duration) 'duration', " +
                                    " sum(customerchargeablesDuration) 'customerchargeablesDuration', sum(customerPayables) 'customerpayables', sum(vendorchargableDuration) 'vendorchargableduration', " +
                                    " sum(vendorReceivables) 'vendorReceivables', sum(averageVendorRate) 'averageVendorRate', sum(margin) 'Margin' " +
                                    " from Days_mera.dbo.dia_actual_acum hr " +
                                    " where ReportDateHour ='" + ReportDateHour +
//                                    "' and duration is not null " +
                                    "' and totalCalls <>0  " +
                                    " group by Customer, vendor, area, customer_area_Description, customer_rate, Vendor_area_description, vendor_rate ";

            SqlCommand cmd_get_hour = new SqlCommand(sql_get_hour, conexion_hour_read);

            SqlDataReader read_hour = cmd_get_hour.ExecuteReader();
            if (read_hour.HasRows)
            {
                while (read_hour.Read())
                {
                    string customer = read_hour["customer"].ToString();
                    string vendor1 = read_hour["vendor"].ToString();
                    string area1 = read_hour["Area"].ToString();
                    string customer_area_description = read_hour["Customer_Area_Description"].ToString();
                    string customer_rate = read_hour["customer_rate"].ToString();
                    string vendor_area_description = read_hour["vendor_area_description"].ToString();
                    string vendor_rate = read_hour["vendor_rate"].ToString();
                    string total_call = read_hour["TotalCalls"].ToString();
                    string SeizedCall1 = read_hour["SeizedCall"].ToString();
                    string SuccesFullCall = read_hour["SuccesFullCall"].ToString();
                    string duration = read_hour["duration"].ToString();
                    string margin = read_hour["margin"].ToString();
                    if (duration.Trim().Length == 0)
                    {
                        duration = "0";
                    }
                    if (margin.Trim().Length == 0)
                    {
                        margin = "0";
                    }
                    string customerchargeablesDuration = read_hour["customerchargeablesDuration"].ToString();
                    if (customerchargeablesDuration.Trim().Length == 0)
                    {
                        customerchargeablesDuration = "0";
                    }
                    string customerpayables = read_hour["customerpayables"].ToString();
                    if (customerpayables.Trim().Length == 0)
                    {
                        customerpayables = "0";
                    }
                    string vendorchargableduration = read_hour["vendorchargableduration"].ToString();
                    if (vendorchargableduration.Trim().Length == 0)
                    {
                        vendorchargableduration = "0";
                    }
                    string vendorReceivables = read_hour["vendorReceivables"].ToString();
                    if (vendorReceivables.Trim().Length == 0)
                    {
                        vendorReceivables = "0";
                    }
                    string averageVendorRate = read_hour["averageVendorRate"].ToString();
                    if (averageVendorRate.Trim().Length == 0)
                    {
                        averageVendorRate = "0";
                    }
                    if (customer_rate.Trim().Length == 0)
                    {
                        customer_rate = "0";
                    }
                    if (vendor_rate.Trim().Length == 0)
                    {
                        vendor_rate = "0";
                    }
                    string Margin1 = read_hour["Margin"].ToString();

                    string sql_get_toUpdate = "select * from Weeks_mera.dbo.semana_actual_acum " +
                                    "where customer='" + customer + "' " +
                                    " and vendor ='" + vendor1 + "' " +
                                    " and area = '" + area1 + "' " +
                                    " and customer_area_description='" + customer_area_description + "' " +
                                    " and customer_rate ='" + customer_rate + "' " +
                                    " and vendor_area_description ='" + vendor_area_description + "' " +
                                    " and vendor_rate ='" + vendor_rate + "' ";

                    SqlCommand cmd_get_toUpdate = new SqlCommand(sql_get_toUpdate, conexion_week_read);
                    SqlDataReader read_toUpdate = cmd_get_toUpdate.ExecuteReader();

                    if (read_toUpdate.HasRows)
                    {
                        string sql_upd_record = "update Weeks_mera.dbo.semana_actual_acum set TotalCalls = TotalCalls +" + Convert.ToInt64(total_call) + "," +
                                               " SeizedCall =SeizedCall +" + Convert.ToInt64(SeizedCall1) + "," +
                                               " SuccesFullCall = SuccesFullCall +" + Convert.ToInt64(SuccesFullCall) + "," +
                                               " duration = duration +" + Convert.ToDecimal(duration) + ", " +
                                               " customerchargeablesDuration = customerchargeablesDuration +" + Convert.ToDecimal(customerchargeablesDuration) + "," +
                                               " customerpayables = customerpayables +" + Convert.ToDecimal(customerpayables) + "," +
                                               " vendorchargableduration = vendorchargableduration +" + Convert.ToDecimal(vendorchargableduration) + "," +
                                               " vendorReceivables = vendorReceivables +" + Convert.ToDecimal(vendorReceivables) + "," +
                                               " averageVendorRate= averageVendorRate +" + Convert.ToDecimal(averageVendorRate) + "," +
                                               " Margin =Margin +" + Convert.ToDecimal(margin) +
                                               "where customer='" + customer + "' " +
                                               " and vendor ='" + vendor1 + "' " +
                                               " and area = '" + area1 + "' " +
                                               " and customer_area_description='" + customer_area_description + "' " +
                                               " and customer_rate ='" + customer_rate + "' " +
                                               " and vendor_area_description ='" + vendor_area_description + "' " +
                                               " and vendor_rate ='" + vendor_rate + "' ";

                        SqlCommand cmd_upd_record = new SqlCommand(sql_upd_record, conexion_week_update);

                        try
                        {
                            int result_upd = cmd_upd_record.ExecuteNonQuery();
                        }
                        catch (Exception E)
                        {
                            logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Error Actualizando registros, en la tabla de acumulados por semana  " + sql_upd_record + " " + E.Message);
                        }

                    }
                    else // No esta el registro, so se va a insertar //
                    {
                        string sql_new_record = " insert into Weeks_mera.dbo.semana_actual_acum values ('" + customer + "','" + vendor1 + "','" + area1 + "','" + customer_area_description + "','" +
                                                        customer_rate + "','" + vendor_area_description + "','" + vendor_rate + "','" + total_call + "','" +
                                                        SeizedCall1 + "','" + SuccesFullCall + "','" + duration + "','" + customerchargeablesDuration + "','" +
                                                        customerpayables + "','" + vendorchargableduration + "','" + vendorReceivables + "','" + averageVendorRate + "','" +
                                                        Margin1 + "','" + ReportDateHour + "','" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "') ";
                        SqlCommand cmd_new_record = new SqlCommand(sql_new_record, conexion_week_update);

                        try
                        {
                            int result_new = cmd_new_record.ExecuteNonQuery();
                        }
                        catch (Exception E)
                        {
                            logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Error al insertar nuevo registro en la tabla de acumulados por semana  " + sql_new_record + " " + E.Message);
                        }
                    }
                    read_toUpdate.Close();
                }
            }
            conexion_hour_read.Close();
            conexion_week_read.Close();
            conexion_week_update.Close();
        }
        static void AcumulaBilateral()
        {
            SqlConnection conexion = new SqlConnection(SqlStringConnection);

            SqlConnection conexion1 = new SqlConnection(SqlStringConnection);

            SqlConnection conexion2 = new SqlConnection(SqlStringConnection);

            conexion.Open();
            conexion1.Open();
            conexion2.Open();

            // se lee la tabla de Bilaterales primero para luego buscar por cliente y areas los minutos //

            string sqlGetBilat = "";
            string customerSearch = "";
            string areaSearch = "";
            string flujoSearch = "";
            string country = "";
            string fieldToSum = "";

            sqlGetBilat = " select  idnum, Customer, CustomersGroup, Area, AreasGroup, fecha_inicio, fecha_fin, flujo, include_exclude_area, include_exclude_customer, cant_minutos, duracion, cond_termino  " +
                                  " from Bilaterals bi " +
                                  " where estatus='A'  " +
                                  " and estatus_area='A' "+
                                  " order by Customer, Flujo, Area ";

            SqlCommand cmdGetBilat = new SqlCommand(sqlGetBilat, conexion);

            SqlDataReader readerBilat = cmdGetBilat.ExecuteReader();

            if (readerBilat.HasRows)
            {
                while (readerBilat.Read())
                {
                    string biCustomer = readerBilat["Customer"].ToString();
                    string customersGroup = readerBilat["CustomersGroup"].ToString();
                    string biArea = readerBilat["area"].ToString();
                    string areasGroup = readerBilat["AreasGroup"].ToString();

                    string includeExcludeArea = readerBilat["include_exclude_area"].ToString();
                    string includeExcludeCustomer = readerBilat["include_exclude_customer"].ToString();

                    string fecha_inicio = Convert.ToDateTime(readerBilat["fecha_inicio"].ToString()).ToString("MM/dd/yyyy");
                    string fecha_fin = Convert.ToDateTime(readerBilat["fecha_fin"].ToString()).ToString("MM/dd/yyyy");

                    //string fecha_inicio = readerBilat["fecha_inicio"].ToString();
                    //string fecha_fin = readerBilat["fecha_fin"].ToString();

                    string flujo = readerBilat["flujo"].ToString();


                    string idNum = readerBilat["idNum"].ToString();

                    string cantMinutos = readerBilat["cant_minutos"].ToString();
                    string biDuracion = readerBilat["duracion"].ToString();
                    string condTermino = readerBilat["cond_termino"].ToString();

                    areaSearch = "";
                    biArea = biArea.Trim();
                    areasGroup = areasGroup.Trim();
                    includeExcludeArea = includeExcludeArea.Trim();
                    includeExcludeCustomer = includeExcludeCustomer.Trim();

                    switch (includeExcludeArea)
                    {

                        case "IN":
                            // You can use the parentheses in a case body.
                            int countAreas = 0;
                            while (areasGroup.IndexOf(",") != -1)
                            {
                                if (countAreas == 0)
                                {
                                    areaSearch = areasGroup.Substring(0, areasGroup.IndexOf(",")).Trim();
                                    areasGroup = areasGroup.Substring(areasGroup.IndexOf(",") + 1).Trim();
                                }
                                else
                                {
                                    areaSearch = areaSearch + "','" + areasGroup.Substring(0, areasGroup.IndexOf(",")).Trim();
                                    areasGroup = areasGroup.Substring(areasGroup.IndexOf(",") + 1).Trim();
                                }
                                countAreas++;
                            }
                            areaSearch = areaSearch + "','" + areasGroup;
                            areaSearch = " and area in ('" + areaSearch + "')";
                            break;
                        case "NOT IN":
                            // You can use the parentheses in a case body.
                            countAreas = 0;
                            while (areasGroup.IndexOf(",") != -1)
                            {
                                if (countAreas == 0)
                                {
                                    areaSearch = areasGroup.Substring(0, areasGroup.IndexOf(",")).Trim();
                                    areasGroup = areasGroup.Substring(areasGroup.IndexOf(",") + 1).Trim();
                                }
                                else
                                {
                                    areaSearch = areaSearch + "','" + areasGroup.Substring(0, areasGroup.IndexOf(",")).Trim();
                                    areasGroup = areasGroup.Substring(areasGroup.IndexOf(",") + 1).Trim();
                                }
                                countAreas++;
                            }
                            areaSearch = areaSearch + "','" + areasGroup;
                            areaSearch = " and area not in ('" + areaSearch + "')";
                            break;
                        case "LIKE":
                            areaSearch = " and area like '%" + areasGroup + "%' ";
                            break;
                        case "NOT LIKE":
                            if (biArea.IndexOf(" ") != -1)
                            {
                                country = biArea.Substring(0, biArea.IndexOf(" ")).Trim();
                            }
                            else
                            {
                                country = biArea;
                            }
                            areaSearch = " and area not like '%" + areasGroup + "%' && area like '%" + country + "%' ";
                            break;
                        case "=":
                            areaSearch = " and area ='" + areasGroup + "' ";
                            break;
                        default:
                            // You can use the default case.
                            areaSearch = " and area ='" + biArea + "' ";
                            break;
                    }
                    switch (includeExcludeCustomer)
                    {
                        case "IN":
                            int countCustomers = 0;
                            while (customersGroup.IndexOf(",") != -1)
                            {
                                if (countCustomers == 0)
                                {
                                    customerSearch = customersGroup.Substring(0, customersGroup.IndexOf(",")).Trim();
                                    customersGroup = customersGroup.Substring(customersGroup.IndexOf(",") + 1).Trim();
                                }
                                else
                                {
                                    customerSearch = customerSearch + "','" + customersGroup.Substring(0, customersGroup.IndexOf(",")).Trim();
                                    customersGroup = customersGroup.Substring(customersGroup.IndexOf(",") + 1).Trim();
                                }
                                countCustomers++;
                            }
                            customerSearch = customerSearch + "','" + customersGroup;
                            customerSearch = " and customer in ('" + customerSearch + "') ";
                            break;
                        case "NOT IN":
                            countCustomers = 0;
                            while (customersGroup.IndexOf(",") != -1)
                            {
                                if (countCustomers == 0)
                                {
                                    customerSearch = customersGroup.Substring(0, customersGroup.IndexOf(",")).Trim();
                                    customersGroup = customersGroup.Substring(customersGroup.IndexOf(",") + 1).Trim();
                                }
                                else
                                {
                                    customerSearch = customerSearch + "','" + customersGroup.Substring(0, customersGroup.IndexOf(",")).Trim();
                                    customersGroup = customersGroup.Substring(customersGroup.IndexOf(",") + 1).Trim();
                                }
                                countCustomers++;
                            }
                            customerSearch = customerSearch + "','" + customersGroup;
                            customerSearch = " and customer not in ('" + customerSearch + "') ";
                            break;

                        case "LIKE":
                            customerSearch = " and customer like '%" + customersGroup + "%' ";
                            break;
                        case "NOT LIKE":
                            customerSearch = " and customer not like '%" + customersGroup + "%' ";
                            break;
                        case "=":
                            customerSearch = " and customer='" + customersGroup + "' ";
                            break;
                        default:
                            customerSearch = " and customer ='" + biCustomer + "' ";
                            break;
                    }
                    if (flujo == "OUT")
                    {
                        customerSearch = customerSearch.Replace("customer", "vendor");
                        flujoSearch = " and flujo ='OUT'";
                        fieldToSum = "VendorChargableDuration";
                    }
                    if (flujo =="IN")
                    {
                        flujoSearch = " and flujo='IN' ";
                        fieldToSum = "customerchargeablesDuration";
                    }

                    // Get minutos de la hora //
                //    areaSearch = areaSearch.Replace("and ", "where ");

                    

                    areaSearch = areaSearch.Replace("&&", " and ");
                    // Aqui se cambia la estructura de la busqueda para trabajar los Other Country de los bilaterales //  09/14/2016 //
                    if (biArea == "Other Countries")
                    {
                        areaSearch = " and Area not in (select Area from TempBilaterals where Customer='" + biCustomer + "' and flujo='" + flujo + "') ";
                    }
                    string sqlGetDuration = "select sum("+fieldToSum+"), sum(margin) " +
                                        " from Days_mera.dbo.day_history " + //areaSearch + customerSearch +
                       //                 " and ReportDateHour>='" + fecha_inicio + "' ";
                                    " where ReportDateHour ='" + ReportDateHour + "' " + areaSearch + customerSearch;

                    logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Buscando para actualizar Bilaterals, programa individual " + sqlGetDuration);

                    SqlCommand cmdGetDuration = new SqlCommand(sqlGetDuration, conexion1);
                    cmdGetDuration.CommandTimeout = 1200;
                    try
                    {
                        //   if (bi_customer =="BTS" && bi_area =="United States")
                        //   {
                        //      Console.ReadLine();
                        //  }
                        SqlDataReader readerDuration = cmdGetDuration.ExecuteReader();
                        if (readerDuration.HasRows)
                        {
                            while (readerDuration.Read())
                            {
                                string durationHora = readerDuration[0].ToString();
                                string margin = readerDuration[1].ToString();
                                if (durationHora.Trim().Length == 0)
                                {
                                    durationHora = "0";
                                }
                                if (margin.Trim().Length == 0)
                                {
                                    margin = "0";
                                }

                                // actualiza bilateral //
                                string modEstatusArea = " ";
                                string CerrarArea = "";
                                if (condTermino == "1")
                                {
                                    if (Convert.ToDecimal(cantMinutos) < (Convert.ToDecimal(durationHora) + Convert.ToDecimal(biDuracion)))
                                    {
                                        modEstatusArea = ", estatus_area='C', fecha_completacion_area = getdate() ";
                                    }
                                }
                                if (condTermino == "2")
                                {
                                    if (Convert.ToDateTime(fecha_fin) <= DateTime.Today)
                                    {
                                        modEstatusArea = ", estatus_area='C', fecha_completacion_area = getdate() ";
                                    }
                                }
                                if (condTermino == "3")
                                {
                                    if ((Convert.ToDecimal(cantMinutos) < (Convert.ToDecimal(durationHora) + Convert.ToDecimal(biDuracion))) || (Convert.ToDateTime(fecha_fin) <= DateTime.Today))
                                    {
                                        modEstatusArea = ", estatus_area='C', fecha_completacion_area = getdate() ";
                                    }
                                }

                                string sqlUpdateBila = "update bilaterals set duracion = duracion  +" + durationHora + ", margin=+ " + margin + modEstatusArea +
                                                    " where idnum='" + idNum + "' ";

                                SqlCommand cmdUpdateBila = new SqlCommand(sqlUpdateBila, conexion2);
                                try
                                {
                                    int result_updHour = cmdUpdateBila.ExecuteNonQuery();
                                    logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + "Sentencia Update =" + sqlUpdateBila);

                                    // Modificado Exitosamente el bilateral se procede a guardar en el historico diario de minutos por bilateral //

                                    string sqlInsertHistoryBila = "Insert into BilateralsHistory.dbo.BilateralsDayly values (" + idNum + ",'" + ReportDateHour + "'," + durationHora + "," + margin + ")";
                                    SqlCommand cmdInsertHistoryBila = new SqlCommand(sqlInsertHistoryBila, conexion2);

                                    try
                                    {
                                        int resultInsertHistoryBila = cmdInsertHistoryBila.ExecuteNonQuery();
                                    }
                                    catch (Exception e)
                                    {
                                        logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Error al Insertar los minutos del dia en el historico de Bilaterales " + sqlInsertHistoryBila+" Error ="+e.Message);
                                    }

                                }
                                catch (Exception E)
                                {
                                    logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Error actualizando registro =" + sqlUpdateBila + " " + E.Message);
                                }
                            }
                            readerDuration.Close();
                        }
                        else
                        {
                            logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " No se encontraron registros para el cliente ='" + customerSearch + " and area ='" + areaSearch);
                        }
                        readerDuration.Close();
                    }
                    catch (Exception E)
                    {
                        logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Error al buscar el trafico ayer, sql =" + sqlGetDuration + " Error =" + E.Message);
                    }
                    
                }
            }
            else
            {
                logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " No se encontraron registros para su proceso ");
            }
            logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Final del Proceso de Calculo de los minutos por Bilateral, programa individual ");
        }
        static void AcumulaBilateralAyer()
        {
            SqlConnection conexion = new SqlConnection(SqlStringConnection);

            SqlConnection conexion1 = new SqlConnection(SqlStringConnection);

            SqlConnection conexion2 = new SqlConnection(SqlStringConnection);

            conexion.Open();
            conexion1.Open();
            conexion2.Open();

            // se lee la tabla de Bilaterales primero para luego buscar por cliente y areas los minutos //
/*
            string sqlCleanBilaterals = "update bilaterals set duracion_ayer=0 where estatus_area='C' and convert(varchar,date_time_stamp,101) < convert(varchar,getdate()-1,101) ";
            SqlCommand cmdCleanBilaterals = new SqlCommand(sqlCleanBilaterals, conexion);
            try
            {
                int resultClean = cmdCleanBilaterals.ExecuteNonQuery();
                logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Pone en 0 Trafico ayer para las areas completadas ");  // Agregado el 08/17/2016
            }
            catch (Exception E)
            {
                logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Error al poner en 0 el trafico ayer para las areas completadas en Bilaterals " + E.Message);
            }
*/
            string sqlGetGilat = "";
            string customerSearch = "";
            string areaSearch = "";
            string flujoSearch = "";
            string country = "";
            string fieldToSum = "";

            sqlGetGilat = " select idNum, Customer, CustomersGroup, Area, AreasGroup, fecha_inicio, fecha_fin, flujo, include_exclude_area, include_exclude_customer  " +
                                  " from Bilaterals bi " +
                                  " where estatus='A'   " +
                       //            " and estatus_area='A' " +
                                  " order by Customer, Flujo, Area ";

            SqlCommand cmdGetBilat = new SqlCommand(sqlGetGilat, conexion);

            SqlDataReader readerBilat = cmdGetBilat.ExecuteReader();

            if (readerBilat.HasRows)
            {
                while (readerBilat.Read())
                {
                    string biCustomer = readerBilat["Customer"].ToString();
                    string customersGroup = readerBilat["CustomersGroup"].ToString();
                    string biArea = readerBilat["area"].ToString();
                    string areasGroup = readerBilat["AreasGroup"].ToString();

                    //          if (bi_customer == "Cable & Wireless Panama" && bi_area == "Dominican Republic - Proper")
                    //         {
                    //             Console.Read();
                    //             int pp = 0;
                    //             pp = pp + 1;
                    //         }



                    string includeExcludeArea = readerBilat["include_exclude_area"].ToString();
                    string includeExcludeCustomer = readerBilat["include_exclude_customer"].ToString();

                    string flujo = readerBilat["flujo"].ToString();

                    string idNum = readerBilat["idNum"].ToString();

                    areaSearch = "";
                    biArea = biArea.Trim();
                    areasGroup = areasGroup.Trim();
                    includeExcludeArea = includeExcludeArea.Trim();
                    includeExcludeCustomer = includeExcludeCustomer.Trim();
                    switch (includeExcludeArea)
                    {

                        case "IN":
                            // You can use the parentheses in a case body.
                            int countAreas = 0;
                            while (areasGroup.IndexOf(",") != -1)
                            {
                                if (countAreas == 0)
                                {
                                    areaSearch = areasGroup.Substring(0, areasGroup.IndexOf(",")).Trim();
                                    areasGroup = areasGroup.Substring(areasGroup.IndexOf(",") + 1).Trim();
                                }
                                else
                                {
                                    areaSearch = areaSearch + "','" + areasGroup.Substring(0, areasGroup.IndexOf(",")).Trim();
                                    areasGroup = areasGroup.Substring(areasGroup.IndexOf(",") + 1).Trim();
                                }
                                countAreas++;
                            }
                            areaSearch = areaSearch + "','" + areasGroup;
                            areaSearch = " and area in ('" + areaSearch + "')";
                            break;
                        case "NOT IN":
                            // You can use the parentheses in a case body.
                            countAreas = 0;
                            while (areasGroup.IndexOf(",") != -1)
                            {
                                if (countAreas == 0)
                                {
                                    areaSearch = areasGroup.Substring(0, areasGroup.IndexOf(",")).Trim();
                                    areasGroup = areasGroup.Substring(areasGroup.IndexOf(",") + 1).Trim();
                                }
                                else
                                {
                                    areaSearch = areaSearch + "','" + areasGroup.Substring(0, areasGroup.IndexOf(",")).Trim();
                                    areasGroup = areasGroup.Substring(areasGroup.IndexOf(",") + 1).Trim();
                                }
                                countAreas++;
                            }
                            areaSearch = areaSearch + "','" + areasGroup;
                            areaSearch = " and area not in ('" + areaSearch + "')";
                            break;
                        case "LIKE":
                            areaSearch = " and area like '%" + areasGroup + "%' ";
                            break;
                        case "NOT LIKE":
                            if (biArea.IndexOf(" ") != -1)
                            {
                                country = biArea.Substring(0, biArea.IndexOf(" ")).Trim();
                            }
                            else
                            {
                                country = biArea;
                            }

                            areaSearch = " and area not like '%" + areasGroup + "%' and area like '%" + country + "%' ";
                            break;
                        case "=":
                            areaSearch = " and area ='" + areasGroup + "' ";
                            break;
                        default:
                            // You can use the default case.
                            areaSearch = " and area ='" + biArea + "' ";
                            break;
                    }
                    switch (includeExcludeCustomer)
                    {
                        case "=":
                            customerSearch = " and customer ='" + customersGroup + "' ";
                            break;
                        case "IN":
                            int countCustomers = 0;
                            while (customersGroup.IndexOf(",") != -1)
                            {
                                if (countCustomers == 0)
                                {
                                    customerSearch = customersGroup.Substring(0, customersGroup.IndexOf(",")).Trim();
                                    customersGroup = customersGroup.Substring(customersGroup.IndexOf(",") + 1).Trim();
                                }
                                else
                                {
                                    customerSearch = customerSearch + "','" + customersGroup.Substring(0, customersGroup.IndexOf(",")).Trim();
                                    customersGroup = customersGroup.Substring(customersGroup.IndexOf(",") + 1).Trim();
                                }
                                countCustomers++;
                            }
                            customerSearch = customerSearch + "','" + customersGroup;
                            customerSearch = " and customer in ('" + customerSearch + "') ";
                            break;
                        case "NOT IN":
                            countCustomers = 0;
                            while (customersGroup.IndexOf(",") != -1)
                            {
                                if (countCustomers == 0)
                                {
                                    customerSearch = customersGroup.Substring(0, customersGroup.IndexOf(",")).Trim();
                                    customersGroup = customersGroup.Substring(customersGroup.IndexOf(",") + 1).Trim();
                                }
                                else
                                {
                                    customerSearch = customerSearch + "','" + customersGroup.Substring(0, customersGroup.IndexOf(",")).Trim();
                                    customersGroup = customersGroup.Substring(customersGroup.IndexOf(",") + 1).Trim();
                                }
                                countCustomers++;
                            }
                            customerSearch = customerSearch + "','" + customersGroup;
                            customerSearch = " and customer not in ('" + customerSearch + "') ";
                            break;

                        case "LIKE":
                            customerSearch = " and customer like '%" + customersGroup + "%' ";
                            break;
                        case "NOT LIKE":
                            customerSearch = " and customer not like '%" + customersGroup + "%' ";
                            break;
                        default:
                            customerSearch = " and customer ='" + biCustomer + "' ";
                            break;
                    }
                    if (flujo == "OUT")
                    {
                        customerSearch = customerSearch.Replace("customer", "vendor");
                        flujoSearch = " and flujo ='OUT'";
                        fieldToSum = "VendorChargableDuration";
                    }
                    if (flujo == "IN")
                    {
                        flujoSearch = " and flujo='IN' ";
                        fieldToSum = "customerchargeablesDuration";
                    }
                    // Get minutos de la hora //
                    string ayer = "convert (varchar,getdate()-1, 111)";
                    //areaSearch = areaSearch.Replace("and ", "where ");
                    areaSearch = areaSearch.Replace("&&", " and ");

                    if (biArea == "Other Countries")
                    {
                        areaSearch = " and Area not in (select Area from TempBilaterals where Customer='" + biCustomer + "' and flujo='" + flujo + "') ";
                    }

                    string sqlGetDuration = "select sum("+fieldToSum+"), sum(margin) " +
                                        " from Days_mera.dbo.day_history " +
                                        " where convert(varchar,ReportDateHour,111) =" + ayer + " " + areaSearch + customerSearch;

                    logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Buscando para actualizar Bilaterals, programa individual " + sqlGetDuration);

                    SqlCommand cmdGetDuration = new SqlCommand(sqlGetDuration, conexion1);

                    cmdGetDuration.CommandTimeout = 1200;

                    SqlDataReader readerDuration = cmdGetDuration.ExecuteReader();
                    if (readerDuration.HasRows)
                    {
                        while (readerDuration.Read())
                        {
                            string durationHora = readerDuration[0].ToString();
                            string margin = readerDuration[1].ToString();
                            if (durationHora.Trim().Length == 0)
                            {
                                durationHora = "0";
                            }
                            if (margin.Trim().Length == 0)
                            {
                                margin = "0";
                            }

                            // actualiza bilateral //
                            string sqlUpdateBila = "update bilaterals set duracion_ayer = " + durationHora +
                                                " where idNum="+idNum;
                            SqlCommand cmdUpdateBila = new SqlCommand(sqlUpdateBila, conexion2);
                            try
                            {
                                int resultUpdHour = cmdUpdateBila.ExecuteNonQuery();
                                logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Sentencia Update =" + sqlUpdateBila);
                            }
                            catch (Exception E)
                            {
                                logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Error actualizando registro =" + sqlUpdateBila + " " + E.Message);
                            }
                        }
                    }
                    else
                    {
                        logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " No se encontraron registros para el cliente ='" + customerSearch + " and area ='" + areaSearch);
                    }
                    readerDuration.Close();

                }
            }
            else
            {
                logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " No se encontraron registros para su proceso ");
            }
            logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Final del Proceso de Calculo de los minutos por Bilateral, programa individual ");
        }
        static void LlenaTemporal()
        {
            logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " *** Inicio Llena Temporal ***");
            try
            {
                SqlConnection conexion = new SqlConnection(SqlStringConnection);

                SqlConnection conexion1 = new SqlConnection(SqlStringConnection);

                SqlConnection conexion2 = new SqlConnection(SqlStringConnection);

                conexion.Open();
                conexion1.Open();
                conexion2.Open();

                int conta = 0;
                int total_acuerdo = 0;

                decimal total_cursado = 0;
                decimal porctotal;
                decimal total_aldia = 0;
                decimal total_faltante = 0;
                decimal total_por_pasar = 0;
                decimal total_proyectado = 0;
                decimal total_margen_esperado = 0;
                decimal total_magen_real = 0;
                decimal total_magen_diferencia = 0;
                decimal totalproyectado = 0;
                decimal total_balance_bilateral = 0;

                decimal totalBalanceAyer = 0;

                decimal porcAldia = 0;
                decimal porc_Aldia = 0;
                string avgCost = "";

                int pedro = 1;

                decimal balanceInCustomer = 0;
                decimal balanceOutCustomer = 0;

                decimal balanceInCustomerAyer = 0;
                decimal balanceOutCustomerAyer = 0;

                string customerDireccion = "";
                string customerDireccionAnterior = "";
                string customerAnterior = "";
                string direccionAnterior = "";

                string sqlInsertTotals = "";

                string sqlGetRecords = "truncate table BilateralsTemp;select estatus,Customer, flujo 'Direc' , Area 'Area', fecha_inicio, fecha_fin,datediff(day,Fecha_inicio, Fecha_fin) 'Cant_Dias',    costo_compra, precio_venta,  rate_efective_cust, rate_efective_vend," +
                                    " datediff(day,Fecha_inicio, getdate()) 'Activo', datediff(day,Fecha_inicio, Fecha_fin) - datediff(day,Fecha_inicio, getdate())+1 'Dias_Faltan', cant_minutos 'Acuerdo', " +
                                    " (duracion/cant_minutos) 'porc_Total', (cant_minutos/datediff(day,Fecha_inicio, Fecha_fin))*datediff(day,Fecha_inicio, getdate()) 'Al_Dia',duracion 'Cursado', " +
                                    " (case  when (datediff(day,Fecha_inicio, Fecha_fin)*datediff(day,Fecha_inicio, getdate()))= 0 then 0 else  (duracion/((cant_minutos/datediff(day,Fecha_inicio, Fecha_fin))*datediff(day,Fecha_inicio, getdate()))) END)  'porc_al_Dia', " +
                    //  " (Cant_minutos - duracion) 'faltante', (case when (Cant_minutos - duracion)<>0 then (Cant_minutos - duracion)/(datediff(day,Fecha_inicio, Fecha_fin) - datediff(day,Fecha_inicio, getdate())) else 0 END) 'por_pasar_por_dia', " +
                                    "  (Cant_minutos - duracion) 'faltante', (case when (Cant_minutos - duracion)<>0 then (case when (datediff(day,Fecha_inicio, Fecha_fin) - datediff(day,Fecha_inicio, getdate()))=0 then 0 else ((Cant_minutos - duracion)/(datediff(day,Fecha_inicio, Fecha_fin) - datediff(day,Fecha_inicio, getdate()))) END) else 0 END) 'por_pasar_por_dia', " +
                                    " duracion_ayer 'ayer', duracion+(duracion_ayer*(datediff(day,Fecha_inicio, Fecha_fin)-datediff(day,Fecha_inicio, getdate()))) 'proyectado'," +
                                    " (case when (duracion =0 or duracion_ayer=0) then 0 else (case when duracion_ayer=0 then ((Cant_minutos - duracion)/(duracion/datediff(day,Fecha_inicio, getdate()))) else (Cant_minutos - duracion)/duracion_ayer end) end)  as 'dias_por_pasar', " +
                                    " duracion *(Precio_venta - Costo_compra) as 'margen_esperado', margin 'margen_real', (duracion *(Precio_venta - Costo_compra)) - margin 'margen_diferencia', " +
                    //                                " (case when Flujo ='IN' then duracion*(rate_efective_cust - Costo_compra) else duracion* (Costo_compra - rate_efective_cust) end ) as 'balance_bilateral' "+
                    //                                " (case when Flujo ='IN' then duracion*(Precio_venta - Costo_compra) else duracion* (Costo_compra - rate_efective_cust) end ) as 'balance_bilateral' " + // Cambiado el 11/14/2016 nuevo calculo JP
                                      " (case when Flujo ='IN' then duracion_ayer*(Precio_venta - rate_efective_vend) else duracion_ayer* (rate_efective_cust - costo_compra) end ) as 'balance_ayer', " +
                                    " (case when Flujo ='IN' then duracion*(Precio_venta - rate_efective_vend) else duracion* (rate_efective_cust - costo_compra) end ) as 'balance_bilateral' " +
                                    " from bilaterals " +
                                    " where estatus='A'" +
                                    " order by customer, flujo,area ";
                SqlCommand cmdGetRecords = new SqlCommand(sqlGetRecords, conexion);
                SqlDataReader readerRecords = cmdGetRecords.ExecuteReader();
                if (readerRecords.HasRows)
                {
                    while (readerRecords.Read())
                    {
                        string estatus = readerRecords["estatus"].ToString();
                        string customer = readerRecords["customer"].ToString();
                        string direccion = readerRecords["direc"].ToString();
                        string area = readerRecords["area"].ToString();
                        string fecha_inicio = readerRecords["fecha_inicio"].ToString();
                        string fecha_fin = readerRecords["fecha_fin"].ToString();
                        string cant_dias = readerRecords["cant_dias"].ToString();
                        string costo_compra = readerRecords["costo_compra"].ToString();
                        string precio_venta = readerRecords["precio_venta"].ToString();
                        string rate_efective_cust = readerRecords["rate_efective_cust"].ToString();

                        avgCost = readerRecords["rate_efective_vend"].ToString();

                        string activo = readerRecords["Activo"].ToString();
                        string dias_faltan = readerRecords["Dias_Faltan"].ToString();
                        string acuerdo = readerRecords["Acuerdo"].ToString();
                        string porc_total = readerRecords["porc_total"].ToString();
                        string al_dia = readerRecords["al_dia"].ToString();
                        string cursado = readerRecords["cursado"].ToString();
                        string porc_al_dia = readerRecords["porc_al_Dia"].ToString();
                        string faltante = readerRecords["faltante"].ToString();
                        string por_pasar_por_dia = readerRecords["por_pasar_por_dia"].ToString();
                        string ayer = readerRecords["ayer"].ToString();
                        string proyectado = readerRecords["proyectado"].ToString();
                        string dias_por_pasar = readerRecords["dias_por_pasar"].ToString();
                        string margen_esperado = readerRecords["margen_esperado"].ToString();
                        string margen_real = readerRecords["margen_real"].ToString();
                        string margen_diferencia = readerRecords["margen_diferencia"].ToString();
                        string balance_bilateral = readerRecords["balance_bilateral"].ToString();
                        string balanceAyer = readerRecords["balance_ayer"].ToString();

                        if (Convert.ToDecimal(dias_faltan) < 0)
                        {
                            dias_faltan = "0";
                        }

                        if (faltante.Trim().Length == 0)
                        {
                            faltante = "0";
                        }
                        if (cursado.Trim().Length == 0)
                        {
                            cursado = "0";
                        }
                        if (porc_al_dia.Trim().Length == 0)
                        {
                            porc_al_dia = "0";
                        }
                        if (proyectado.Trim().Length == 0)
                        {
                            proyectado = "0";
                        }

                        if (dias_faltan.Trim().Length == 0)
                        {
                            por_pasar_por_dia = faltante;
                        }
                        if (Convert.ToDecimal(acuerdo) <= Convert.ToDecimal(cursado))
                        {
                            porc_total = "1";

                            porc_al_dia = "1";
                            proyectado = "0";
                            dias_por_pasar = "0";
                            por_pasar_por_dia = "0";
                        }
                        customerDireccion = customer + direccion;
                        if (conta == 0)
                        {
                            customerDireccionAnterior = customerDireccion;
                            customerAnterior = customer;
                            direccionAnterior = direccion;
                        }

                        if (customerDireccionAnterior != customerDireccion)
                        {
                            porctotal = total_cursado / total_acuerdo;
                            if (total_aldia != 0)
                            {
                                porc_Aldia = total_cursado / total_aldia;
                            }
                            else
                            {
                                porc_Aldia = 0;
                            }

                            sqlInsertTotals = "insert into bilateralsTemp (estatus,customer, direccion, area, fecha_inicio, fecha_fin, cant_dias, costo_compra, precio_venta, rate_efective_cust, avg_cost, activo, " +
                                " dias_faltan, acuerdo, porciento_total, al_dia, cursado, porc_al_dia, faltante, por_pasar_por_dia, ayer, proyectado, dias_por_pasar, margen_esperado, margen_real, margen_diferencia,Balance_ayer, Balance_bilateral)" +
                                " values ('A','" + customerAnterior + "',null,null,null,null,null,null,null,null,null,null,null," + total_acuerdo + "," + porctotal + "," + total_aldia + "," + total_cursado + "," + porc_Aldia + ",null,null,null,null,null," + totalproyectado +
                                 total_margen_esperado + "," + total_magen_real + "," + total_magen_diferencia + "," + totalBalanceAyer + "," + total_balance_bilateral + ")";

                            if (direccionAnterior == "IN")
                            {
                                balanceInCustomer = total_balance_bilateral;
                                balanceInCustomerAyer = totalBalanceAyer;
                            }
                            else
                            {
                                balanceOutCustomer = total_balance_bilateral;
                                balanceOutCustomerAyer = totalBalanceAyer;
                            }

                            SqlCommand cmdInsertTotals = new SqlCommand(sqlInsertTotals, conexion1);

                            int resultado = cmdInsertTotals.ExecuteNonQuery();


                            if (customerAnterior != customer)
                            {
                                // Inserta registro en blanco //

                            //    decimal absbalanceOutCustomer = Math.Abs(balanceOutCustomer);
                            //    decimal absbalanceOutCustomerAyer = Math.Abs(balanceOutCustomerAyer);

                                decimal diferenciaInOut = balanceInCustomer - Math.Abs(balanceOutCustomer);
                                decimal diferenciaInOutAyer = balanceInCustomerAyer - Math.Abs(balanceOutCustomerAyer);
                                
                          //      decimal diferenciaInOut = Math.Abs(balanceInCustomer) - Math.Abs(absbalanceOutCustomer);
                          //      decimal diferenciaInOutAyer = Math.Abs(balanceInCustomerAyer) - Math.Abs(absbalanceOutCustomerAyer);

                                string sqlInsertBlank = "insert into bilateralsTemp (estatus,customer, direccion, area, fecha_inicio, fecha_fin, cant_dias, costo_compra, precio_venta, rate_efective_cust, avg_cost,activo, " +
                                        " dias_faltan, acuerdo, porciento_total, al_dia, cursado, porc_al_dia, faltante, por_pasar_por_dia, ayer, proyectado, dias_por_pasar, margen_esperado, margen_real, margen_diferencia,Balance_Ayer, Balance_bilateral)" +
                                        " values ('A','" + customerAnterior + "',null,null,null,null,null,null, null, null, null,null, null,null, null, null, null, null,null, null, null, null, null,null, null, null," + diferenciaInOutAyer + "," + diferenciaInOut + ")";
                                SqlCommand cmdInsertBlank = new SqlCommand(sqlInsertBlank, conexion1);

                                int resInsertBlank = cmdInsertBlank.ExecuteNonQuery();

                                sqlInsertBlank = "insert into bilateralsTemp (estatus,customer, direccion, area, fecha_inicio, fecha_fin, cant_dias, costo_compra, precio_venta, rate_efective_cust, avg_cost,activo, " +
                                   " dias_faltan, acuerdo, porciento_total, al_dia, cursado, porc_al_dia, faltante, por_pasar_por_dia, ayer, proyectado, dias_por_pasar, margen_esperado, margen_real, margen_diferencia,Balance_ayer, Balance_bilateral)" +
                                   " values ('A'" + ",null,null,null,null,null,null,null, null, null, null,null, null,null, null, null, null, null,null, null, null, null, null,null, null, null, null,null)";

                                cmdInsertBlank = new SqlCommand(sqlInsertBlank, conexion1);

                                resInsertBlank = cmdInsertBlank.ExecuteNonQuery();

                                balanceInCustomerAyer = 0;
                                balanceOutCustomerAyer = 0;

                                balanceOutCustomer = 0;
                                balanceInCustomer = 0;
                            }
                            total_acuerdo = 0;
                            total_cursado = 0;
                            total_aldia = 0;
                            total_faltante = 0;
                            total_por_pasar = 0;

                            total_margen_esperado = 0;
                            total_magen_real = 0;
                            total_magen_diferencia = 0;
                            total_balance_bilateral = 0;
                            totalBalanceAyer = 0;

                            totalproyectado = 0;


                        }
                        if (direccion == "OUT")
                        {
                            avgCost = "0";
                        }
                        string sqlInsert = "insert into bilateralsTemp (estatus,customer, direccion, area, fecha_inicio, fecha_fin, cant_dias, costo_compra, precio_venta, rate_efective_cust,avg_cost, activo, " +
                            " dias_faltan, acuerdo, porciento_total, al_dia, cursado, porc_al_dia, faltante, por_pasar_por_dia, ayer, proyectado, dias_por_pasar, margen_esperado, margen_real, margen_diferencia,Balance_ayer, Balance_bilateral)" +
                            " values ('" + estatus + "','" + customer + "','" + direccion + "','" + area + "','" + fecha_inicio + "','" + fecha_fin + "','" + cant_dias + "','" + costo_compra + "','" + precio_venta + "','" + rate_efective_cust + "','" + avgCost + "','" + activo +
                            "','" + dias_faltan + "','" + acuerdo + "','" + porc_total + "','" + al_dia + "','" + cursado + "','" + porc_al_dia + "','" + faltante + "','" + por_pasar_por_dia + "','" + ayer + "','" + proyectado + "','" + dias_por_pasar +
                            "','" + margen_esperado + "','" + margen_real + "','" + margen_diferencia + "','" + balanceAyer + "','" + balance_bilateral + "')";
                        SqlCommand cmdInsert = new SqlCommand(sqlInsert, conexion1);

                        int resInsert = cmdInsert.ExecuteNonQuery();

                        total_acuerdo = total_acuerdo + Convert.ToInt32(acuerdo);
                        total_cursado = total_cursado + Convert.ToDecimal(cursado);
                        total_aldia = total_aldia + Convert.ToDecimal(al_dia);
                        total_faltante = total_faltante + Convert.ToDecimal(faltante);
                        total_por_pasar = total_por_pasar + Convert.ToDecimal(por_pasar_por_dia);

                        total_margen_esperado = total_margen_esperado + Convert.ToDecimal(margen_esperado);
                        total_magen_real = total_magen_real + Convert.ToDecimal(margen_real);
                        total_magen_diferencia = total_magen_diferencia + Convert.ToDecimal(margen_diferencia);

                        total_balance_bilateral = total_balance_bilateral + Convert.ToDecimal(balance_bilateral);
                        totalBalanceAyer = totalBalanceAyer + Convert.ToDecimal(balanceAyer);

                        totalproyectado = totalproyectado + Convert.ToDecimal(total_proyectado);
                        customerDireccionAnterior = customerDireccion;
                        customerAnterior = customer;
                        direccionAnterior = direccion;
                        conta++;
                    }
                    porctotal = total_cursado / total_acuerdo;
                    if (total_aldia != 0)
                    {
                        porcAldia = total_cursado / total_aldia;
                    }
                    else
                    {
                        porcAldia = 0;
                    }
                    if (direccionAnterior == "IN")
                    {
                        balanceInCustomer = total_balance_bilateral;
                        balanceInCustomerAyer = totalBalanceAyer;
                    }
                    else
                    {
                        balanceOutCustomer = total_balance_bilateral;
                        balanceOutCustomerAyer = totalBalanceAyer;
                    }
                    sqlInsertTotals = "insert into bilateralsTemp (estatus,customer, direccion, area, fecha_inicio, fecha_fin, cant_dias, costo_compra, precio_venta, rate_efective_cust, avg_cost, activo, " +
                                            " dias_faltan, acuerdo, porciento_total, al_dia, cursado, porc_al_dia, faltante, por_pasar_por_dia, ayer, proyectado, dias_por_pasar, margen_esperado, margen_real, margen_diferencia,Balance_ayer, Balance_bilateral)" +
                                            " values ('A','" + customerAnterior + "',null,null,null,null,null,null,null,null,null,null,null," + total_acuerdo + "," + porctotal + "," + total_aldia + "," + total_cursado + "," + porc_Aldia + ",null,null,null,null,null," + totalproyectado +
                                            total_margen_esperado + "," + total_magen_real + "," + total_magen_diferencia + "," + totalBalanceAyer + "," + total_balance_bilateral + ")";
                    SqlCommand cmdInsertTotal1s = new SqlCommand(sqlInsertTotals, conexion1);

                    int resulta = cmdInsertTotal1s.ExecuteNonQuery();

               //     decimal abs_balanceOutCustomer = Math.Abs(balanceOutCustomer);
               //     decimal abs_balanceOutCustomerAyer = Math.Abs(balanceOutCustomerAyer);

                    decimal diferenciaInOutFinal = balanceInCustomer - Math.Abs(balanceOutCustomer);
                    decimal diferenciaInOutAyerFinal = balanceInCustomerAyer - Math.Abs(balanceOutCustomerAyer);

                //    decimal diferenciaInOutFinal = Math.Abs(balanceInCustomer) - Math.Abs(abs_balanceOutCustomer);
                 //   decimal diferenciaInOutAyerFinal = Math.Abs(balanceInCustomerAyer) - Math.Abs(abs_balanceOutCustomerAyer);


                    string sqlInsertTotalsFinal = "insert into bilateralsTemp (estatus,customer, direccion, area, fecha_inicio, fecha_fin, cant_dias, costo_compra, precio_venta, rate_efective_cust, avg_cost,activo, " +
                            " dias_faltan, acuerdo, porciento_total, al_dia, cursado, porc_al_dia, faltante, por_pasar_por_dia, ayer, proyectado, dias_por_pasar, margen_esperado, margen_real, margen_diferencia,Balance_Ayer, Balance_bilateral)" +
                            " values ('A','" + customerAnterior + "',null,null,null,null,null,null, null, null, null,null, null,null, null, null, null, null,null, null, null, null, null,null, null, null," + diferenciaInOutAyerFinal + "," + diferenciaInOutFinal + ")";
                    //      string sqlInsertTotalsFinal = "insert into bilateralsTemp (estatus,customer, direccion, area, fecha_inicio, fecha_fin, cant_dias, costo_compra, precio_venta, rate_efective_cust, activo, " +
                    //          " dias_faltan, acuerdo, porciento_total, al_dia, cursado, porc_al_dia, faltante, por_pasar_por_dia, ayer, proyectado, dias_por_pasar, margen_esperado, margen_real, margen_diferencia,Balance_Ayer, Balance_bilateral)" +
                    //         " values ('A','" + customerAnterior + "',null,null,null,null,null,null,null,null,null,null," + total_acuerdo + "," + porctotal + "," + total_aldia + "," + total_cursado + "," + porcAldia + ",null,null,null,null,null," + totalproyectado +
                    //          total_margen_esperado + "," + total_magen_real + "," + total_magen_diferencia +","+ totalBalanceAyer+","+ total_balance_bilateral+ ")";
                    SqlCommand cmdInsertBalanceFinal = new SqlCommand(sqlInsertTotalsFinal, conexion1);

                    try
                    {
                        int resultadoFinal = cmdInsertBalanceFinal.ExecuteNonQuery();
                    }
                    catch (Exception e2)
                    {
                        logFile.WriteLine(DateTime.Now.ToString("MM/dd/yyyy HH:MM:ss") + " Error Insertando Linea Balance final en la tabla BilateralsTemp  " + sqlInsertTotalsFinal + " Error=" + e2.Message);
                    }

                }
                else
                {
                    logFile.WriteLine(DateTime.Now.ToString("MM/dd/yyyy HH:MM:ss") + " Error No se pudo Crear la tabla temporal, favor revisar  ");
                }
            }
            catch (Exception e9)
            {
                logFile.WriteLine(DateTime.Now.ToString("MM/dd/yyyy HH:MM:ss") + " Error en metodo Lllena Temporal   Error=" + e9.Message);
            }
            logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " *** Fin Llena Temporal ***");
             
        }

        static void EnviaEmail()
        {

            //   string notification_email = "SalesSms@identidadtelecom.net,noc@identidadtelecom.net";
            string notificationEmail = "jcarmona@identidadtelecom.net";
            string sujeto = "No File where process today/Error en Proceso de Carga ";
           
            System.Net.Mail.MailMessage msg = new System.Net.Mail.MailMessage();
            msg.To.Add(notificationEmail);
            //      msg.CC.Add("jcarmona@identidadtelecom.net");
            msg.From = new MailAddress("alarma@identidadtelecom.net", "Carga Minutos Mera", System.Text.Encoding.UTF8);
            msg.Subject = sujeto;
            msg.Body = sujeto;
            msg.SubjectEncoding = System.Text.Encoding.UTF8;
            msg.BodyEncoding = System.Text.Encoding.UTF8;
            msg.IsBodyHtml = true;
            //     msg.Attachments.Add(new Attachment(fileToAttach));
            msg.Priority = MailPriority.High;
            SmtpClient client = new SmtpClient();
            //client.Credentials = new System.Net.NetworkCredential("systemrates@identidadtelecom.net", "PQd8GKQL");
            client.Credentials = new System.Net.NetworkCredential("alarma@identidadtelecom.net", "FTOG4Bcs+m");
            client.Port = 587;
            //  client.Port = 25;
            //            client.Host = "smtp.gmail.com";
            // client.Host = "smtpout.secureserver.net";
            client.Host = "mail.identidadtelecom.net";  // identidad
            client.EnableSsl = false; //Esto es para que vaya a través de SSL que es obligatorio con GMail
            try
            {
                client.Send(msg);
                logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Email enviado Existosamente..... ");
            }
            catch (SmtpException ex)
            {
                logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + sujeto +" "+ ex.Message);
            }

        }
        static void GetCostAvg()
        {

            logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Entrada a Calculo de Average del costo ");
            try
            {
                SqlConnection conexion = new SqlConnection(SqlStringConnection);

                SqlConnection conexion2 = new SqlConnection(SqlStringConnection);

                SqlConnection conexion3 = new SqlConnection(SqlStringConnection);

                conexion.Open();

                conexion2.Open();

                conexion3.Open();

                string sqlGetGilat = "";
                string customerSearch = "";
                string areaSearch = "";
                string flujoSearch = "";
                string country;
                string promedioProfit = "";
                string sqlGetDuration = "";
                string sqlGetPromedio = "";
                string completacionFecha = "";                 

                sqlGetGilat = " select  idnum, estatus_area, fecha_completacion_area, Customer, CustomersGroup, Area, AreasGroup, fecha_inicio, fecha_fin, flujo, costo_compra, rate_efective_cust, include_exclude_area, include_exclude_customer, cant_minutos, duracion, cond_termino  " +
                                      " from Bilaterals bi " +
                                      " where estatus='A'  " +
                    //  " and estatus_area='A'"+  Para Calcular el avg no se debe filtrar por  area, solo el estatus del bila // 11/15/2016 //         
                                     " and flujo='IN' " +
                                     " order by Customer, Flujo, Area ";

                SqlCommand cmdGetGilat = new SqlCommand(sqlGetGilat, conexion);

                SqlDataReader readerBilat = cmdGetGilat.ExecuteReader();

                if (readerBilat.HasRows)
                {
                    while (readerBilat.Read())
                    {
                        string biCustomer = readerBilat["Customer"].ToString();
                        string customersGroup = readerBilat["CustomersGroup"].ToString();
                        string biArea = readerBilat["area"].ToString();
                        string areasGroup = readerBilat["AreasGroup"].ToString();

                        string includeExcludeArea = readerBilat["include_exclude_area"].ToString();
                        string includeExcludeCustomer = readerBilat["include_exclude_customer"].ToString();

                        string fechaInicio = Convert.ToDateTime(readerBilat["fecha_inicio"].ToString()).ToString("yyyy/MM/dd");
                        string fechaFin = Convert.ToDateTime(readerBilat["fecha_fin"].ToString()).ToString("yyyy/MM/dd");

                        string rateEfectiveCust = readerBilat["rate_efective_cust"].ToString();
                        string costoCompra = readerBilat["costo_compra"].ToString();

                        string flujo = readerBilat["flujo"].ToString();

                        string idNum = readerBilat["idNum"].ToString();

                        string cantMinutos = readerBilat["cant_minutos"].ToString();
                        string biDuracion = readerBilat["duracion"].ToString();
                        string condTermino = readerBilat["cond_termino"].ToString();

                        string estatusArea = readerBilat["estatus_area"].ToString();
                        string fechaCompletacionArea = readerBilat["fecha_completacion_area"].ToString();

                        if (fechaCompletacionArea.Trim().Length != 0)
                        {
                            completacionFecha = Convert.ToDateTime(fechaCompletacionArea).ToString("yyyy/MM/dd");
                        }
                        else
                        {
                            completacionFecha = "2099/12/31";
                        }

                        string fechaHoy = DateTime.Today.ToString("yyyy/MM/dd");

                        if (estatusArea == "A" || (estatusArea == "C" && fechaHoy == completacionFecha))
                        {
                            logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Se calcula Avg " + biCustomer + " " + biArea + " " + estatusArea + " " + completacionFecha);
                            areaSearch = "";
                            biArea = biArea.Trim();
                            areasGroup = areasGroup.Trim();
                            includeExcludeArea = includeExcludeArea.Trim();
                            includeExcludeCustomer = includeExcludeCustomer.Trim();

                            switch (includeExcludeArea)
                            {

                                case "IN":
                                    // You can use the parentheses in a case body.
                                    int countAreas = 0;
                                    while (areasGroup.IndexOf(",") != -1)
                                    {
                                        if (countAreas == 0)
                                        {
                                            areaSearch = areasGroup.Substring(0, areasGroup.IndexOf(",")).Trim();
                                            areasGroup = areasGroup.Substring(areasGroup.IndexOf(",") + 1).Trim();
                                        }
                                        else
                                        {
                                            areaSearch = areaSearch + "','" + areasGroup.Substring(0, areasGroup.IndexOf(",")).Trim();
                                            areasGroup = areasGroup.Substring(areasGroup.IndexOf(",") + 1).Trim();
                                        }
                                        countAreas++;
                                    }
                                    areaSearch = areaSearch + "','" + areasGroup;
                                    areaSearch = " and area in ('" + areaSearch + "')";
                                    break;
                                case "NOT IN":
                                    // You can use the parentheses in a case body.
                                    countAreas = 0;
                                    while (areasGroup.IndexOf(",") != -1)
                                    {
                                        if (countAreas == 0)
                                        {
                                            areaSearch = areasGroup.Substring(0, areasGroup.IndexOf(",")).Trim();
                                            areasGroup = areasGroup.Substring(areasGroup.IndexOf(",") + 1).Trim();
                                        }
                                        else
                                        {
                                            areaSearch = areaSearch + "','" + areasGroup.Substring(0, areasGroup.IndexOf(",")).Trim();
                                            areasGroup = areasGroup.Substring(areasGroup.IndexOf(",") + 1).Trim();
                                        }
                                        countAreas++;
                                    }
                                    areaSearch = areaSearch + "','" + areasGroup;
                                    areaSearch = " and area not in ('" + areaSearch + "')";
                                    break;
                                case "LIKE":
                                    areaSearch = " and area like '%" + areasGroup + "%' ";
                                    break;
                                case "NOT LIKE":

                                    if (biArea.IndexOf(" ") != -1)
                                    {
                                        country = biArea.Substring(0, biArea.IndexOf(" ")).Trim();
                                    }
                                    else
                                    {
                                        country = biArea;
                                    }
                                    areaSearch = " and area not like '%" + areasGroup + "%' && area like '" + country + "%' ";
                                    break;
                                case "=":
                                    areaSearch = " and area ='" + areasGroup + "' ";
                                    break;
                                default:
                                    // You can use the default case.
                                    areaSearch = " and area ='" + biArea + "' ";
                                    break;
                            }
                            switch (includeExcludeCustomer)
                            {
                                case "IN":
                                    int countCustomers = 0;
                                    while (customersGroup.IndexOf(",") != -1)
                                    {
                                        if (countCustomers == 0)
                                        {
                                            customerSearch = customersGroup.Substring(0, customersGroup.IndexOf(",")).Trim();
                                            customersGroup = customersGroup.Substring(customersGroup.IndexOf(",") + 1).Trim();
                                        }
                                        else
                                        {
                                            customerSearch = customerSearch + "','" + customersGroup.Substring(0, customersGroup.IndexOf(",")).Trim();
                                            customersGroup = customersGroup.Substring(customersGroup.IndexOf(",") + 1).Trim();
                                        }
                                        countCustomers++;
                                    }
                                    customerSearch = customerSearch + "','" + customersGroup;
                                    customerSearch = " and customer in ('" + customerSearch + "') ";
                                    break;
                                case "NOT IN":
                                    countCustomers = 0;
                                    while (customersGroup.IndexOf(",") != -1)
                                    {
                                        if (countCustomers == 0)
                                        {
                                            customerSearch = customersGroup.Substring(0, customersGroup.IndexOf(",")).Trim();
                                            customersGroup = customersGroup.Substring(customersGroup.IndexOf(",") + 1).Trim();
                                        }
                                        else
                                        {
                                            customerSearch = customerSearch + "','" + customersGroup.Substring(0, customersGroup.IndexOf(",")).Trim();
                                            customersGroup = customersGroup.Substring(customersGroup.IndexOf(",") + 1).Trim();
                                        }
                                        countCustomers++;
                                    }
                                    customerSearch = customerSearch + "','" + customersGroup;
                                    customerSearch = " and customer not in ('" + customerSearch + "') ";
                                    break;

                                case "LIKE":
                                    customerSearch = " and customer like '%" + customersGroup + "%' ";
                                    break;
                                case "NOT LIKE":
                                    customerSearch = " and customer not like '%" + customersGroup + "%' ";
                                    break;
                                case "=":
                                    customerSearch = " and customer='" + customersGroup + "' ";
                                    break;
                                default:
                                    customerSearch = " and customer ='" + biCustomer + "' ";
                                    break;
                            }
                            if (flujo == "OUT")
                            {
                                customerSearch = customerSearch.Replace("customer", "vendor");
                                flujoSearch = " and flujo='OUT' ";
                            }
                            if (flujo == "IN")
                            {
                                flujoSearch = " and flujo='IN' ";
                            }

                            areaSearch = areaSearch.Replace("and ", "where ");
                            areaSearch = areaSearch.Replace("&&", " and ");

                            // Aqui se cambia la estructura de la busqueda para trabajar los Other Country de los bilaterales //  09/14/2016 //
                            if (biArea == "Other Countries")
                            {
                                areaSearch = "where Area not in (select Area from TempBilaterals where Customer='" + biCustomer + "' and flujo='" + flujo + "') ";
                            }
                            // Get minutos de la hora //


                            // Luego de actualizado los minutos se busca el promedio de costo desde el mera para los IN
                            if (condTermino == "2")
                            {
                                sqlGetPromedio = "select ( CASE when sum(traffic)=0 then 0 else (sum(efect_buy_rate*traffic)/sum(traffic)  ) end ) as avg " +
                                                        " from CostosEfectivos.dbo.CostosAcumulados " + areaSearch + customerSearch +
                                                        " and convert(varchar,date_time_stamp,111) between '" + fechaInicio + "' and '" + fechaFin + "' ";
                            }
                            else
                            {
                                if (fechaCompletacionArea.Trim().Length != 0)
                                {
                                    sqlGetPromedio = "select ( CASE when sum(traffic) is null then 0 else (sum(efect_buy_rate*traffic)/sum(traffic)  ) end ) as avg " +
                                                " from CostosEfectivos.dbo.CostosAcumulados " + areaSearch + customerSearch +
                                                " and convert(varchar,date_time_stamp,111) between '" + fechaInicio + "' and  '" + completacionFecha + "' ";
                                }
                                else
                                {
                                    sqlGetPromedio = "select ( CASE when sum(traffic) is null then 0 else (sum(efect_buy_rate*traffic)/sum(traffic)  ) end ) as avg " +
                                                            " from CostosEfectivos.dbo.CostosAcumulados " + areaSearch + customerSearch +
                                                            " and convert(varchar,date_time_stamp,111) >='" + fechaInicio + "' ";
                                }
                            }
                            logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Busqueda del promedio =" + sqlGetPromedio);
                            SqlCommand cmdGetPromedio = new SqlCommand(sqlGetPromedio, conexion3);

                            cmdGetPromedio.CommandTimeout = 1200;

                            SqlDataReader readGetPromedio = cmdGetPromedio.ExecuteReader();
                            readGetPromedio.Read();                          

                            string promedioCosto = readGetPromedio[0].ToString();

                            if (promedioCosto.Trim().Length ==0)
                            {
                                promedioCosto = "0";
                            }

                            string sqlUpdateBilaPromedio = "update bilaterals set rate_efective_vend =" + promedioCosto +
                                        " where idnum='" + idNum + "' ";

                            SqlCommand UpdBilaPromedio = new SqlCommand(sqlUpdateBilaPromedio, conexion2);
                            try
                            {
                                int resulUpdPromedio = UpdBilaPromedio.ExecuteNonQuery();
                                logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Actulizacion del promedio =" + sqlUpdateBilaPromedio);
                            }
                            catch (Exception e1)
                            {
                                logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Error Actualizando promedio =" + sqlUpdateBilaPromedio + " " + e1.Message);
                            }
                            readGetPromedio.Close();
                        }
                        else
                        {
                            logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " No Se calcula Avg " + biCustomer + " " + biArea + " " + estatusArea + " " + fechaCompletacionArea);
                        }
                    }
                }
                else
                {
                    logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " No se encontraron registros para el cliente ='" + customerSearch + " and area ='" + areaSearch);
                }
                conexion.Close();
                conexion2.Close();
                conexion3.Close();
                conexion.Dispose();
                conexion2.Dispose();
                conexion3.Dispose();
            }
            catch (Exception e8)
            {
                logFile.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Error en Metodo Calcula avg " + e8.Message);
                EnviaEmail();
            }
        }


    }
}
