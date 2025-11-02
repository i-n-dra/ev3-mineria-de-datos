library(dplyr)
library(tidyr)
library(lubridate)
library(readxl)
library(arrow)
library(here)
library(janitor)
library(stringi)
library(stringr)
library(writexl)

# importación del archivo ventas.xlsx
raw_xlsx <- here::here("ev3", "otros", "ventas.xlsx")
ventas_df <- readxl::read_excel(
  path = raw_xlsx,
  sheet = "Ventas"
)

# inspección de datos
# head(ventas_df)
# View(ventas_df)

productos_replace <- c(
  "\\bcamara\\b"      = "Cámara",
  "\\bfutbol\\b"      = "Fútbol",
  "\\blampara\\b"     = "Lámpara",
  "\\btelefono\\b"    = "Teléfono",
  "\\bplastico\\b"    = "Plástico",
  "\\belectronico\\b" = "Electrónico",
  "\\bsillon\\b"      = "Sillón",
  "\\bbalon\\b"       = "Balón",
  "\\bbateria\\b"     = "Batería",
  "\\bportatil\\b"    = "Portátil",
  "\\brapido\\b"      = "Rápido",
  "\\bbasico\\b"      = "Básico",
  "\\binalambrico\\b" = "Inalámbrico"
)

payment_replace <- c(
  "\\befectiv\\b"      = "Efectivo",
  "\\btarjeta\\b|\\btarjta\\b"      = "Tarjeta",
  "\\bcredito\\b"     = "Crédito"
)

shipping_replace <- c(
  "\\bexprees\\b" = "Express",
  "(retiro( en)?( tienda)?)" = "Retiro en Tienda",
  "\\bnorml\\b|\\bnormal\\b" = "Normal",
  "\\beconomico\\b" = "Económico"
)

# fechas normalizar y ordenar antiguo -> nuevo
ventas_df <- ventas_df %>%
  mutate(
         date = coalesce(
           suppressWarnings(dmy(date)),
           suppressWarnings(ymd(date)),
           suppressWarnings(mdy(date))
         )) %>%
  filter(!is.na(date)) %>%
  complete(date = seq(min(date), max(date), by = "day")) %>%
  mutate(date = format(date, "%Y/%m/%d")) %>%
  arrange(date)
ventas_df <- ventas_df %>%
  mutate(
    product_name = product_name %>%
      trimws() %>%
      stri_trans_nfc() %>%
      str_to_lower() %>%
      str_to_title(locale = "es") %>%
      str_replace_all(setNames(
        productos_replace,
        lapply(names(productos_replace),
               function(p) regex(p, ignore_case = TRUE))
      )) %>%
      str_replace_all(regex("\\bHd\\b", ignore_case = TRUE), "HD") %>%
      str_replace_all(regex("\\bLed\\b", ignore_case = TRUE), "LED") %>%
      str_replace_all(regex("\\bUsb\\b", ignore_case = TRUE), "USB")
  )
ventas_df <- ventas_df %>%
  mutate(
    category = category %>%
      trimws() %>%
      stri_trans_nfc() %>%
      str_to_lower() %>%
      str_to_title(locale = "es") %>%
      str_replace("^A.*", "Accesorios") %>%
      str_replace("^D.*", "Deporte") %>%
      str_replace("^Ele.*", "Electrónica")
  )
ventas_df <- ventas_df %>%
  mutate(
    payment_method = payment_method %>%
      trimws() %>%
      stri_trans_nfc() %>%
      str_to_lower() %>%
      str_replace_all(setNames(
        payment_replace,
        lapply(names(payment_replace),
               function(p) regex(p, ignore_case = TRUE))
      )) %>%
      str_to_title(locale = "es")
  )
ventas_df <- ventas_df %>%
  mutate(
    shipping_type = shipping_type %>%
      trimws() %>%
      stri_trans_nfc() %>%
      str_to_lower() %>%
      str_squish() %>%
      str_replace_all(setNames(
        shipping_replace,
        lapply(names(shipping_replace),
               function(p) regex(p, ignore_case = TRUE))
      )) %>%
      str_to_title(locale = "es")
  )
ventas_df <- ventas_df %>%
  mutate(
    product_id = {
      pid <- product_id %>%
        trimws() %>%
        stri_trans_nfc() %>%
        str_to_upper()
      pid <- str_replace(pid, regex("^SKU(?!-)", ignore_case = FALSE), "SKU-")
      pid <- str_replace(pid, regex("^([0-9].*)$"), "SKU-\\1")
      pid
    }
  )
ventas_df <- ventas_df %>%
  mutate(
    quantity = {
      qua <- quantity %>%
        trimws()
      qua[is.na(qua)] <- "0"
      qua <- type.convert(qua, as.is = TRUE, dec = ".")
      qua <- round(qua)
      qua
    }
  )
ventas_df <- ventas_df %>%
  mutate(
    price = {
      pri <- price %>%
        trimws()
      pri <- str_replace(pri, regex("^\\$ "), "")
      pri <- str_replace(pri, regex("\\.", ignore_case = FALSE), "")
      pri[is.na(pri)] <- "0"
      pri <- type.convert(pri, as.is = TRUE, dec = ",")
      pri <- round(pri)
      pri
    }
  )
ventas_df <- ventas_df %>%
  mutate(
    discount = {
      dis <- discount %>%
        trimws()
      dis <- str_replace(dis, regex("%"), "")
      dis <- str_replace(dis, regex("\\.", ignore_case = FALSE), "")
      dis[is.na(dis)] <- "0"
      dis <- type.convert(dis, as.is = TRUE, dec = ".")
      mult <- !str_detect(as.character(discount), "^[^0\\.[1-9]$]|^[^[1-9]$]") # lo q mas entiendo aca es el regex
      mult[is.na(mult)] <- FALSE
      dis[mult] <- dis[mult] / 100
      dis
    }
  )
ventas_df %>% # elimina duplicados
  distinct(order_id, product_id, .keep_all = FALSE)

# NA normalización, limpiar campos en blanco
ventas_df <- ventas_df[rowSums(is.na(ventas_df)) != ncol(ventas_df), ]

ventas_subset <- ventas_df[, c("order_id", "product_id")]
ventas_by_column <- ventas_df[complete.cases(ventas_subset), ]
ventas_df <- ventas_by_column
ventas_empty_int <- ventas_df[-(which(ventas_df$price %in% "0")), ]
ventas_empty_int <- ventas_df[-(which(ventas_df$quantity %in% "0")), ]
ventas_df <- ventas_empty_int

# change the column classes whenever it is appropriate (int, char, etc)
ventas_df <- type.convert(ventas_df, as.is = TRUE)

write.csv(ventas_df, file = "ventas_limpias.csv", dec = ",")