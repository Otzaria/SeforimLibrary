package io.github.kdroidfilter.seforimlibrary.search

import java.nio.file.Files
import java.nio.file.Path
import java.util.Base64

internal object OnnxTestModel {
    fun <T> withModel(block: (Path) -> T): T {
        val directory = Files.createTempDirectory("onnx-lifecycle-")
        val model = directory.resolve("identity.onnx")
        try {
            Files.write(model, Base64.getDecoder().decode(IDENTITY_MODEL))
            return block(model)
        } finally {
            Files.deleteIfExists(model)
            Files.deleteIfExists(directory)
        }
    }

    // Self-contained ONNX IR 8 / opset 13: Identity(float32[1,4] input) -> output.
    // Generated from the public ONNX protobuf schema; no downloaded model is required.
    private const val IDENTITY_MODEL =
        "CAgSG3NlZm9yaW0tb25ueC1saWZlY3ljbGUtdGVzdDpdChkKBWlucHV0EgZvdXRwdXQiCElkZW50aXR5" +
            "Eg10aW55LWlkZW50aXR5WhcKBWlucHV0Eg4KDAgBEggKAggBCgIIBGIYCgZvdXRwdXQSDgoMCAESCAoCCAEKAggEQgIQDQ=="
}
