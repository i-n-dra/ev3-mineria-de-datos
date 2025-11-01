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
raw_xlsx <- here::here("ev3", "ventas.xlsx")
ventas_df <- readxl::read_excel(
  path = raw_xlsx,
  sheet = "Ventas"
)

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
    price = {
      pid <- price %>%
        trimws()
      pid <- str_replace(pid, regex("^\\$ "), "")
      pid <- str_replace(pid, regex("^\\s+"), "0") # falta probar
      pid <- str_replace(pid, regex("\\.", ignore_case = FALSE), "")
      pid <- type.convert(pid, as.is, dec = ",")
      pid <- round(pid)
      pid
    }
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

ventas_df %>% # elimina duplicados
  distinct(order_id, product_id, .keep_all = FALSE)

# NA normalización, limpiar campos en blanco
ventas_df <- ventas_df[rowSums(is.na(ventas_df)) != ncol(ventas_df), ]

ventas_subset <- ventas_df[, c("order_id", "product_id")]
ventas_by_column <- ventas_df[complete.cases(ventas_subset), ]
ventas_df <- ventas_by_column

# change the column classes whenever it is appropriate (int, char, etc)
ventas_df <- type.convert(ventas_df, as.is = TRUE)

write_xlsx(ventas_df, path = "ventas_clean.xlsx")