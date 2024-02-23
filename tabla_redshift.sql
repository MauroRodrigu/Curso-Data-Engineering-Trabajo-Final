CREATE TABLE IF NOT EXISTS rodriguez_mauro11_coderhouse.bcra (
          fecha DATE,
          dolar_oficial FLOAT,
          dolar_paralelo FLOAT,
          brecha_dolar_oficial_paralelo FLOAT,
          base_monetaria FLOAT,
          reservas_internacionales FLOAT,
          base_monetaria_dividida_reservas FLOAT,
          circulacion_monetaria FLOAT,
          depositos FLOAT,
          cuentas_corrientes FLOAT,
          tasa_adelantos_cuenta_corriente FLOAT,
          cajas_ahorro FLOAT,
          plazos_fijos FLOAT,
          tasa_depositos_30_dias FLOAT,
          prestamos FLOAT,
          tasa_prestamos_personales FLOAT,
          porc_prestamos_vs_depositos FLOAT,
          merval FLOAT,
          merval_usd FLOAT
        )
        sortkey(fecha);