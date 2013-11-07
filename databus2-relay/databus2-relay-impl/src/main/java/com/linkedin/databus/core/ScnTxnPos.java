package com.linkedin.databus.core;

public class ScnTxnPos
{
  private String _file;
  private long _fileOffset;
  private long _lineNumber; // Useful for manual debugging
  private long _lineOffset; // Useful for manual debugging
  private long _minScn;
  private long _maxScn;

  
  public void copyFrom(ScnTxnPos original)
  {
    _file = original.getFile();
    _fileOffset = original.getFileOffset();
    _lineNumber = original.getLineNumber();
    _lineOffset = original.getLineOffset();
    _minScn = original.getMinScn();
    _maxScn = original.getMaxScn();

  }
  

  @Override
  public String toString()
  {
    return "ScnTxnPos [_file=" + _file + ", _fileOffset=" + _fileOffset
        + ", _lineNumber=" + _lineNumber + ", _lineOffset=" + _lineOffset + ", _minScn="
        + _minScn + ", _maxScn=" + _maxScn + "]";
  }


  @Override
  public int hashCode()
  {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((_file == null) ? 0 : _file.hashCode());
    result = prime * result + (int) (_fileOffset ^ (_fileOffset >>> 32));
    result = prime * result + (int) (_maxScn ^ (_maxScn >>> 32));
    result = prime * result + (int) (_minScn ^ (_minScn >>> 32));
    return result;
  }



  @Override
  public boolean equals(Object obj)
  {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ScnTxnPos other = (ScnTxnPos) obj;
    if (_file == null)
    {
      if (other._file != null)
        return false;
    }
    else if (!_file.equals(other._file))
      return false;
    if (_fileOffset != other._fileOffset)
      return false;
    if (_maxScn != other._maxScn)
      return false;
    if (_minScn != other._minScn)
      return false;
    return true;
  }



  public boolean isEmpty()
  {
    return 0 > _maxScn;
  }


  public String getFile()
  {
    return _file;
  }

  public void setFile(String file)
  {
    this._file = file;
  }

  public long getFileOffset()
  {
    return _fileOffset;
  }

  public void setFileOffset(long fileOffset)
  {
    this._fileOffset = fileOffset;
  }

  public long getLineNumber()
  {
    return _lineNumber;
  }

  public void setLineNumber(long lineNumber)
  {
    this._lineNumber = lineNumber;
  }

  public long getLineOffset()
  {
    return _lineOffset;
  }

  public void setLineOffset(long lineOffset)
  {
    this._lineOffset = lineOffset;
  }

  public long getMaxScn()
  {
    return _maxScn;
  }
  
  public long getMinScn()
  {
    return _minScn;
  }

  public void setMaxScn(long scn)
  {
    this._maxScn = scn;
  }
  
  public void setMinScn(long scn)
  {
    this._minScn = scn;
  }
}