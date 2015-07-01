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
        bucket = self.state._get_bucket("bucket")

        # Add two objects with "index_key" index values
        fake_object1 = FakeRiakObject(self.state, "bucket", "mykey")
        fake_object2 = FakeRiakObject(self.state, "bucket", "mykey2")
        fake_object1.add_index("index_key", "index_value")
        fake_object2.add_index("index_key", "index_value2")
        self.state.store_object(fake_object1)
        self.state.store_object(fake_object2)

        # Remove the index value for the first object and store it again
        fake_object1.remove_index("index_key")
        fake_object1.add_index("newkey", "newvalue")
        self.state.store_object(fake_object1)

        # Assert that the first object has been removed from the index
        index = bucket["indexes"]["index_key"]
        is_object1_key = lambda (_, key): key == fake_object1.key
        self.assertFalse(any(filter(is_object1_key, index)))
