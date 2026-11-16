package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals

class SefariaBundledDefaultCommentatorsTest {
    private val config = loadDefaultCommentatorsConfig(
        SefariaBundledDefaultCommentatorsTest::class.java.classLoader,
        Json,
        Logger.withTag("test")
    )

    @Test
    fun bavaBatraListsRashbamAfterRashiAndTosafot() {
        assertEquals(
            listOf("רשי על בבא בתרא", "תוספות על בבא בתרא", "רשבם על בבא בתרא"),
            config["בבא בתרא"]
        )
    }

    @Test
    fun nedarimListsRanAfterRashiAndTosafot() {
        assertEquals(
            listOf("רשי על נדרים", "תוספות על נדרים", "רן על נדרים"),
            config["נדרים"]
        )
    }
}
