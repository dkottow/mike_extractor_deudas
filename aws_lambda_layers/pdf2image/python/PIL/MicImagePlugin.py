#
# The Python Imaging Library.
# $Id$
#
# Microsoft Image Composer support for PIL
#
# Notes:
#       uses TiffImagePlugin.py to read the actual image streams
#
# History:
#       97-01-20 fl     Created
#
# Copyright (c) Secret Labs AB 1997.
# Copyright (c) Fredrik Lundh 1997.
#
# See the README file for information on usage and redistribution.
#


import olefile

from . import Image, TiffImagePlugin

#
# --------------------------------------------------------------------


def _accept(prefix):
    return prefix[:8] == olefile.MAGIC


##
# Image plugin for Microsoft's Image Composer file format.


class MicImageFile(TiffImagePlugin.TiffImageFile):

    format = "MIC"
    format_description = "Microsoft Image Composer"
    _close_exclusive_fp_after_loading = False

    def _open(self):

        # read the OLE directory and see if this is a likely
        # to be a Microsoft Image Composer file

        try:
            self.ole = olefile.OleFileIO(self.fp)
        except OSError as e:
            raise SyntaxError("not an MIC file; invalid OLE file") from e

        # find ACI subfiles with Image members (maybe not the
        # best way to identify MIC files, but what the... ;-)

        self.images = []
        for path in self.ole.listdir():
            if path[1:] and path[0][-4:] == ".ACI" and path[1] == "Image":
                self.images.append(path)

        # if we didn't find any images, this is probably not
        # an MIC file.
        if not self.images:
            raise SyntaxError("not an MIC file; no image entries")

        self.__fp = self.fp
        self.frame = None
        self._n_frames = len(self.images)
        self.is_animated = self._n_frames > 1

        if len(self.images) > 1:
            self.category = Image.CONTAINER

        self.seek(0)

    def seek(self, frame):
        if not self._seek_check(frame):
            return
        try:
            filename = self.images[frame]
        except IndexError as e:
            raise EOFError("no such frame") from e

        self.fp = self.ole.openstream(filename)

        TiffImagePlugin.TiffImageFile._open(self)

        self.frame = frame

    def tell(self):
        return self.frame

    def _close__fp(self):
        try:
            if self.__fp != self.fp:
                self.__fp.close()
        except AttributeError:
            pass
        finally:
            self.__fp = None


#
# --------------------------------------------------------------------

Image.register_open(MicImageFile.format, MicImageFile, _accept)

Image.register_extension(MicImageFile.format, ".mic")
