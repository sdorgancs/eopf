

from io import FileIO
from dataclasses import dataclass
import os.path as path
from glob import glob
from typing import Any, Dict, TextIO
import numpy as np
import shutil
import rioxarray as rxr
import xarray as xr
import fs
from fs import copy
from fs.osfs import OSFS
from fs.base import FS
import zarr
import xmltodict
from flatten_dict import flatten
from flatten_dict.reducers import make_reducer

@dataclass
class Dataset:
    metadata: Dict[str, Any]
    data: xr.Dataset


@np.vectorize
def convert_date(s: str) -> np.datetime64:
    from datetime import datetime
    return datetime.strptime(s, r'%Y-%m-%dT%H:%M:%S.%fZ')


def rename_dimensions_and_variables(array: xr.DataArray or xr.Dataset, source)-> xr.DataArray or xr.Dataset:
    new_names = {dim: f'{source}.{dim}' for dim in array.dims}
    if isinstance(array, xr.Dataset):
        new_names.update({var: f'{source}.{var}' for var in array.variables})
    
    return array.rename(new_names)


def clean_attrs(array: xr.DataArray or xr.Dataset) -> xr.DataArray or xr.Dataset:
    if isinstance(array, xr.DataArray):
        if 'scale_factor' in array.attrs and array.attrs['scale_factor'] == 1:
            del array.attrs['scale_factor']
        if 'add_offset' in array.attrs and array.attrs['add_offset'] == 0:
            del array.attrs['add_offset']
        return array
    if isinstance(array, xr.Dataset):
        # array = xr.Dataset(array)
        for var, values in array.items():
            print(var, values.dtype)
            clean_attrs(values)
        return array
    return NotImplementedError()


def save(ds: xr.DataArray, storage_path: str):
    compressor = zarr.BZ2(level=2)
    encoding = {v: {"compressor": compressor} for v in ds.variables.keys()}
    ds.to_zarr(storage_path, encoding=encoding)


def s1_band_name(file_name: str) -> str:
    tokens = file_name.split('-')
    return f'{tokens[0]}_{tokens[1]}_{tokens[3]}_{tokens[3]}'


def s3_band_name(file_name: str) -> str:
    return path.splitext(file_name)[0]


def s2_band_name(file_name: str) -> str:
    name = path.splitext(file_name)[0]
    tokens = name.split('_')
    return f'{tokens[-2]}_{tokens[-1]}'


def simple_name(file_name: str) -> str:
    name = path.splitext(file_name)[0]
    return path.basename(name)


def filter_null_attribute(metadata: Dict[str, str]) -> Dict[str, str]:
    def convert(value):
        if value is None:
            return ""
        return value
    return {k: convert(metadata[k]) for k in metadata}


def read_metadata(fs: FS) -> Dict[str, Any]:
    xml_matches = fs.glob('**/*.xml')
    safe_matches = fs.glob('**/*.safe')
    matches = []
    matches.extend(safe_matches)
    matches.extend(xml_matches)
    metadata = {}
    for match in matches:
        with fs.open(match.path) as file:
            d = xmltodict.parse(file.read())
            metadata[simple_name(match.path)] = d
    import json
    with open("metadata.json", "w") as f:
        json.dump(metadata, f)
    return filter_null_attribute(flatten(metadata, reducer=make_reducer(delimiter='.')))


def load_S1_IW_SLC_zip(s1_file_name: str) -> Dataset:
    with fs.open_fs(s1_file_name) as zip_fs:
        metadata = read_metadata(zip_fs)
        matches = zip_fs.glob(f'**/*.tiff')
        data_vars = {}
        for match in matches:
            img_path = f'{s1_file_name}!{match.path}'
            with rxr.open_rasterio(img_path, chunks='auto') as array:
                band = s1_band_name(path.basename(match.path))
                data_vars[band] = clean_attrs(rename_dimensions_and_variables(array, band))
        return Dataset(data=xr.Dataset(data_vars), metadata=metadata)


def load_S2_MSIL2A_zip(s2_file_name: str) -> Dataset:
    with fs.open_fs(s2_file_name) as zip_fs:
        metadata = read_metadata(zip_fs)
        matches = zip_fs.glob(f'**/*.jp2')
        data_vars = {}
        for match in matches:
            img_path = f'{s2_file_name}!{match.path}'
            with rxr.open_rasterio(img_path, chunks='auto') as array:
                band = s2_band_name(path.basename(match.path))
                data_vars[band] = clean_attrs(rename_dimensions_and_variables(array, band))
        return Dataset(data=xr.Dataset(data_vars), metadata=metadata)


def load_S3_SL_1_RBT_zip(s3_file_name: str) -> Dataset:
    from tempfile import mkdtemp
    ds_list = []
    with fs.open_fs(s3_file_name) as zip_fs:
        metadata = read_metadata(zip_fs)
        tmp_dir = mkdtemp()
        copy.copy_dir(zip_fs, '/', OSFS('/'), tmp_dir)
        matches = glob(f'{tmp_dir}/**/*.nc')
        for match in matches:
            with xr.load_dataset(match, decode_times=False, decode_cf=False, decode_coords=False, mask_and_scale=False, decode_timedelta=False, chunks={}) as array:
                source = s3_band_name(path.basename(match))
                ds = rename_dimensions_and_variables(array, source)
                if source == "viscal":
                    ds['viscal.calibration_time'] = xr.apply_ufunc(convert_date, ds['viscal.calibration_time'])
                    ds['viscal.ANX_time'] = xr.apply_ufunc(convert_date, ds['viscal.ANX_time'])
                ds_list.append(ds)
        ds = xr.merge(ds_list)
        ds = ds.chunk(chunks='auto')
        shutil.rmtree(tmp_dir)
        return Dataset(data=ds, metadata=metadata)
        

def load(url: str) -> Dataset:
    import re
    from urllib.parse import urlparse
    o = urlparse(url)
    file = path.normpath(path.join(o.netloc, o.path))
    if re.match('.*\.zarr', url):
        return xr.open_zarr(file, decode_times=False, chunks='auto')
    elif re.match('S1._IW_SLC_.*', file):
        return load_S1_IW_SLC_zip(url)
    elif re.match('S2._MSIL2A_.*', file):
        return load_S2_MSIL2A_zip(url)
    elif re.match('S3._SL_1_RBT_.*', file):
        return load_S3_SL_1_RBT_zip(url)
    else:
        raise NotImplementedError()

def extract_dataset_definition(ds: xr.Dataset, fd: TextIO):
    fd.write("# Attributes\n")
    for name in ds.attrs:
        fd.write(f'\t{name}={ds.attrs[name]}\n')
    fd.write('# Variables\n')
    for name, value in ds.variables.items():
        fd.write(f'\t{name}: array[{value.data.dtype}]\n')


if __name__ == "__main__":
    # from ray.util.dask import ray_dask_get
    # import ray
    # import dask
    # ray.init()
    # dask.config.set(scheduler=ray_dask_get)

    def test(file_name):
        import json
        # shutil.rmtree(f'{file_name}.zarr', ignore_errors=True)
        # ds = load(f'zip://{file_name}')
        # start = time.time()
        # save(ds, f'{file_name}.zarr')
        # del ds
        # print('writing time', time.time() - start)
        # start = time.time()
        ds = load(f'zip://{file_name}')
        with open(f'{file_name}.json', 'w') as f:
            json.dump(ds.metadata, f, indent=4)
        print(ds.data)
        # variable_number = len(ds2.variables)
        # varkey = list(ds2.variables.keys())[int(variable_number/2)]
        # ds2[varkey].load()
        # with open(f"{file_name}.txt", 'w') as f:
        #     extract_dataset_definition(ds2, f)
        # print('decompression time', time.time() - start)


    # s1_file_name = 'S1B_IW_SLC__1SDV_20201205T164012_20201205T164039_024569_02EBC5_E259.zip'
    # test(s1_file_name)

    # s2_file_name = 'S2B_MSIL2A_20201201T073259_N0214_R049_T40VFK_20201205T164530.zip'
    # test(s2_file_name)

    s3_file_name = 'S3A_SL_1_RBT____20201205T171027_20201205T171327_20201205T190948_0179_066_012_2520_LN2_O_NR_004.zip'
    test(s3_file_name)
