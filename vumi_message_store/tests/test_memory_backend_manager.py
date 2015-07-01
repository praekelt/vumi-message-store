"""
Tests for vumi_message_store.memory_backend_manager.
"""
from vumi.tests.helpers import VumiTestCase
from vumi_message_store.memory_backend_manager import (
    FakeRiakState, FakeRiakObject)


class TestFakeRiakState(VumiTestCase):

    def setUp(self):
        self.state = FakeRiakState(is_sync=False)
        self.add_cleanup(self.state.teardown)

    def test_old_objects_removed_from_indexes(self):
        """
        When a stored object removes an index and is stored again, the old
        index must not reference the object any more
        """
        bucket = self.state._get_bucket("bucket")

        # Add two objects with "index_key" index values
        object1 = FakeRiakObject(self.state, "bucket", "mykey")
        object2 = FakeRiakObject(self.state, "bucket", "mykey2")
        object1.add_index("index_key", "index_value")
        object2.add_index("index_key", "index_value2")
        self.state.store_object(object1)
        self.state.store_object(object2)

        # Remove the index value for the first object and store it again
        object1.remove_index("index_key")
        object1.add_index("newkey", "newvalue")
        self.state.store_object(object1)

        # Assert that the first object has been removed from the index
        index = bucket["indexes"]["index_key"]
        object1_index_values = [(v, k) for v, k in index if k == object1.key]
        self.assertEqual(object1_index_values, [])
