from unittest import TestCase, main

from ods_tools.oed.required_fields.enums.file_type import FileType


class TestFileType(TestCase):

    def setUp(self) -> None:
        self.Acc = 'Acc'
        self.Loc = 'Loc'
        self.ReinsScope = 'ReinsScope'

    def tearDown(self) -> None:
        pass

    def test___init__(self):
        self.assertEqual(FileType(self.Acc).value, self.Acc)
        self.assertEqual(FileType(self.Loc).value, self.Loc)
        self.assertEqual(FileType(self.ReinsScope).value, self.ReinsScope)

        with self.assertRaises(ValueError):
            FileType('invalid')


if __name__ == '__main__':
    main()
