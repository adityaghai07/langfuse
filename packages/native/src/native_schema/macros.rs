//! Implement `clickhouse_table_schema!`, called by `native_schema.rs`.
//!
//! Its helper macros generate the two row structs and their field-reading,
//! conversion, media-restoration and ClickHouse column methods from one field list.

macro_rules! column_name {
    ($field:ident) => {
        stringify!($field)
    };
    ($field:ident as $name:literal) => {
        $name
    };
}
macro_rules! logical_type {
    ($native:ty, $logical:ty) => {
        $logical
    };
    ($native:ty) => {
        $native
    };
}
macro_rules! read_owned {
    ($row:ident, $field:ident, $ty:ty, $name:expr, $policy:expr, metadata) => {
        crate::native_owned::default_string(
            $row.$field,
            $policy,
            &$row.metadata_names,
            &$row.metadata_values,
        )
    };
    ($row:ident, $field:ident, $ty:ty, $name:expr, $policy:expr) => {
        crate::native_owned::into_prepared::<$ty, _>($row.$field, $name)?
    };
}
macro_rules! restore_field {
    ($row:ident, $field:ident, $restore:ident, resolved) => {};
    ($row:ident, $field:ident, $restore:ident) => {
        crate::native_owned::restore_field(&mut $row.$field, $restore)?;
    };
}

macro_rules! apply_attribute {
    ($row:ident, $field:ident, $attributes:ident, text [$($key:expr),*]) => {
        $row.$field = $attributes.text(stringify!($field), &[$($key),*]);
    };
    ($row:ident, $field:ident, $attributes:ident, $method:ident [$($key:expr),*]) => {
        $row.$field = $attributes.$method(&[$($key),*]);
    };
    ($row:ident, $field:ident, $attributes:ident) => {};
}

// These policies describe the pre-insert TypeScript record used for dispatch capacity.
macro_rules! dispatch_entry {
    ($map:ident, $row:ident, $key:expr, $field:ident, if_present) => {
        if let Some(value) = &$row.$field {
            $map.serialize_entry($key, value)?;
        }
    };
    ($map:ident, $row:ident, $key:expr, $field:ident, empty_string) => {
        $map.serialize_entry($key, $row.$field.as_deref().unwrap_or_default())?;
    };
    ($map:ident, $row:ident, $key:expr, $field:ident, pricing) => {
        if $row
            .provided_model_name
            .as_deref()
            .is_some_and(|name| !name.is_empty())
            || !$row.provided_usage_details.is_empty()
            || !$row.provided_cost_details.is_empty()
        {
            $map.serialize_entry($key, &$row.$field)?;
        }
    };
    ($map:ident, $row:ident, $key:expr, $field:ident, evaluation_context) => {
        if $row.has_evaluation_context {
            $map.serialize_entry($key, &$row.$field)?;
        }
    };
    ($map:ident, $row:ident, $key:expr, $field:ident, model_parameters) => {
        let text = $row.$field.as_deref().unwrap_or("{}");
        match serde_json::from_str::<&serde_json::value::RawValue>(text) {
            Ok(json) => $map.serialize_entry($key, json)?,
            Err(_) => $map.serialize_entry($key, text)?,
        }
    };
    ($map:ident, $row:ident, $key:expr, $field:ident) => {
        $map.serialize_entry($key, &$row.$field)?;
    };
}

macro_rules! define_row_types {
    ($($field:ident $(as $name:literal)? $(dispatch($dispatch:ident))? $(logical($logical_ty:ty))? : $ty:ty),* $(,)?) => {
        #[derive(Debug)]
        pub struct PreparedEventRow { $(pub(crate) $field: $ty,)* }

        #[derive(Clone, Debug, Default, Serialize)]
        pub struct OtelStorageRow {
            #[serde(skip)]
            pub(crate) has_evaluation_context: bool,
            $(
                $(#[serde(rename = $name)])?
                pub(crate) $field: logical_type!($ty $(, $logical_ty)?),
            )*
        }

        impl Serialize for TraceDispatchRecord<'_> {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                use serde::ser::SerializeMap;
                let row = self.0;
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("id", &row.span_id)?;
                $(dispatch_entry!(map, row, column_name!($field $(as $name)?), $field $(, $dispatch)?);)*
                map.end()
            }
        }

        impl PreparedEventRow {
            pub(crate) fn retained_bytes(&self) -> usize {
                std::mem::size_of::<Self>() $(+
                    crate::native_memory::retained_size(&self.$field))*
            }
        }

        impl OtelStorageRow {
            pub(crate) fn retained_bytes(&self) -> usize {
                std::mem::size_of::<Self>() $(+
                    crate::native_memory::retained_size(&self.$field))*
            }
        }
    };
}

macro_rules! impl_prepared_row {
    ($($field:ident $(as $name:literal)? : $ty:ty => $policy:expr),* $(,)?) => {
        impl PreparedEventRow {
            pub(crate) fn columns() -> Vec<(String, String, bool)> {
                vec![$((column_name!($field $(as $name)?).into(),
                    crate::native_columns::column_type::<$ty>().to_string(),
                    $policy != DefaultPolicy::None),)*]
            }

            pub(crate) fn schema() -> ch_core_rs::schema::Schema {
                ch_core_rs::schema::Schema::new(vec![$(
                    ch_core_rs::schema::Field {
                        name: column_name!($field $(as $name)?).into(),
                        ch_type: crate::native_columns::column_type::<$ty>(),
                    },
                )*])
            }

            pub(crate) fn build_columns(rows: &[std::sync::Arc<Self>])
                -> Result<Vec<ch_core_rs::column::Column>, crate::native_columns::NativeColumnError>
            {
                Ok(vec![$(crate::native_columns::build_column::<$ty, _>(rows.iter().map(|row| &row.$field))?,)*])
            }
        }
    };
}

macro_rules! impl_otel_row {
    ($(
        $field:ident $(as $name:literal)?
        $(otel($method:ident [$($key:expr),*]))?
        $(media($media:ident))? $(default($default:ident))?
        : $ty:ty => $policy:expr
    ),* $(,)?) => {
        impl OtelStorageRow {
            /// Fill the declared attribute mappings. Event construction supplies IDs,
            /// ingestion context and provider-specific fields afterward.
            pub fn from_attributes(attributes: &mut crate::otel::attributes::EventAttributes<'_>) -> Self {
                let mut row = Self::default();
                $(apply_attribute!(row, $field, attributes $(, $method [$($key),*])?);)*
                row
            }

            pub fn restore_media(&mut self, restore: &mut impl FnMut(&mut String) -> Result<(), String>) -> Result<(), String> {
                $(restore_field!(self, $field, restore $(, $media)?);)*
                Ok(())
            }

            pub fn into_prepared(self) -> Result<PreparedEventRow, String> {
                Ok(PreparedEventRow { $(
                    $field: read_owned!(self, $field, $ty, column_name!($field $(as $name)?), $policy $(, $default)?),
                )* })
            }
        }
    };
}

macro_rules! clickhouse_table_schema {
    ($(
        $field:ident $(as $name:literal)? $(dispatch($dispatch:ident))? $(logical($logical_ty:ty))?
        $(otel($method:ident [$($key:expr),*]))?
        $(media($media:ident))? $(default($default:ident))?
        : $ty:ty => $policy:expr
    ),* $(,)?) => {
        define_row_types! { $($field $(as $name)? $(dispatch($dispatch))? $(logical($logical_ty))?: $ty,)* }
        impl_prepared_row! { $($field $(as $name)?: $ty => $policy,)* }
        impl_otel_row! {
            $($field $(as $name)? $(otel($method [$($key),*]))?
                $(media($media))? $(default($default))?: $ty => $policy,)*
        }
    };
}
