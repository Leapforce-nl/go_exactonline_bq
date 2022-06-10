package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	bigquery "cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	financial "github.com/leapforce-libraries/go_exactonline_new/financial"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type GLScheme struct {
	OrganisationID_          int64
	SoftwareClientLicenceID_ int64
	Created_                 time.Time
	Modified_                time.Time
	ID                       string
	Code                     string
	Created                  bigquery.NullTimestamp
	Creator                  string
	CreatorFullName          string
	Description              string
	Division                 int32
	Main                     byte
	Modified                 bigquery.NullTimestamp
	Modifier                 string
	ModifierFullName         string
	TargetNamespace          string
}

func getGLScheme(c *financial.GLScheme, organisationID int64, softwareClientLicenceID int64) GLScheme {
	t := time.Now()

	return GLScheme{
		organisationID,
		softwareClientLicenceID,
		t, t,
		c.ID.String(),
		c.Code,
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Description,
		c.Division,
		c.Main,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.TargetNamespace,
	}
}

func (service *Service) WriteGLSchemes(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.FinancialService().NewGetGLSchemesCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for {
		glSchemes, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if glSchemes == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGuid()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *glSchemes {
			batchRowCount++

			b, err := json.Marshal(getGLScheme(&tl, organisationID, softwareClientLicenceID))
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write data
			_, err = w.Write(b)
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write NewLine
			_, err = fmt.Fprintf(w, "\n")
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
		}

		if batchRowCount > batchSize {
			// Close and flush data
			err := w.Close()
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
			w = nil

			fmt.Printf("#GLSchemes for flushed: %v\n", batchRowCount)

			rowCount += batchRowCount
			batchRowCount = 0
		}
	}

	if w != nil {
		// Close and flush data
		err := w.Close()
		if err != nil {
			return nil, 0, nil, errortools.ErrorMessage(err)
		}

		rowCount += batchRowCount
	}

	fmt.Printf("#GLSchemes: %v\n", rowCount)

	return objectHandles, rowCount, GLScheme{}, nil
}
