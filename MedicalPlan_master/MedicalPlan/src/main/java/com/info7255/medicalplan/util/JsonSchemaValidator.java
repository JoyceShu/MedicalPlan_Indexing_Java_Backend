package com.info7255.medicalplan.util;

import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;


@Service
public class JsonSchemaValidator {
    public void validateJson(JSONObject object) throws IOException {
        try (InputStream inputStream = getClass().getResourceAsStream("/JsonSchema.json")) {
            JSONObject rawSchema = new JSONObject(new JSONTokener(inputStream));
            Schema schema = SchemaLoader.load(rawSchema);
            schema.validate(object);
        }
    }
}
